##
# Copyright (c) 2024, Your Name
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions, and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions, and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of the author of this software nor the name of
#     contributors to this software may be used to endorse or promote products
#     derived from this software without specific prior written consent.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

###

import json
import urllib.request
import urllib.parse
import urllib.error
import concurrent.futures
import time
from functools import lru_cache
from supybot import utils, plugins, ircutils, callbacks, log, conf
from supybot.commands import *
try:
    from supybot.i18n import PluginInternationalization
    _ = PluginInternationalization('SrrDB')
except ImportError:
    # Placeholder that allows to run the plugin on a bot
    # without the i18n module
    _ = lambda x: x

class SrrDB(callbacks.Plugin):
    """Search srrDB for scene releases"""
    threaded = True

    def __init__(self, irc):
        self.__parent = super(SrrDB, self)
        self.__parent.__init__(irc)
        # Cache for SFV existence checks (release_name -> bool)
        self._sfv_cache = {}
        # Cache for M3U existence checks (release_name -> bool)
        self._m3u_cache = {}
        self._cache_timeout = 300  # 5 minutes
        self._last_cache_clear = time.time()

    def _clear_old_cache(self):
        """Clear cache periodically to avoid memory buildup"""
        current_time = time.time()
        if current_time - self._last_cache_clear > self._cache_timeout:
            self._sfv_cache.clear()
            self._m3u_cache.clear()
            self._last_cache_clear = current_time

    @lru_cache(maxsize=128)
    def _make_api_request(self, url):
        """Make a cached API request"""
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Limnoria SrrDB Plugin/1.1')
            
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode('utf-8'))
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as e:
            log.error(f"SrrDB: API request failed for {url}: {e}")
            return None

    def _search_single_type(self, query, confirmed):
        """Search for either confirmed or unconfirmed releases"""
        encoded_query = urllib.parse.quote(query)
        confirmed_param = "yes" if confirmed else "no"
        url = f"https://api.srrdb.com/v1/search/r:{encoded_query}/confirmed:{confirmed_param}"
        
        data = self._make_api_request(url)
        if data and 'results' in data and data['results']:
            for result in data['results']:
                result['confirmed'] = confirmed_param
            return data['results']
        return []

    def _search_srrdb(self, query):
        """Search srrDB API for releases - both confirmed and unconfirmed"""
        try:
            # Use ThreadPoolExecutor for concurrent requests
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # Submit both searches concurrently
                future_confirmed = executor.submit(self._search_single_type, query, True)
                future_unconfirmed = executor.submit(self._search_single_type, query, False)
                
                # Collect results
                results = []
                
                # Get confirmed results
                try:
                    confirmed_results = future_confirmed.result(timeout=8)
                    if confirmed_results:
                        results.extend(confirmed_results)
                except Exception as e:
                    log.error(f"SrrDB: Error getting confirmed results: {e}")
                
                # Get unconfirmed results
                try:
                    unconfirmed_results = future_unconfirmed.result(timeout=8)
                    if unconfirmed_results:
                        results.extend(unconfirmed_results)
                except Exception as e:
                    log.error(f"SrrDB: Error getting unconfirmed results: {e}")
                
                return results
                
        except Exception as e:
            log.error(f"SrrDB: Unexpected error in search: {e}")
            return None

    def _check_file_exists(self, release_name, file_type, cache_dict):
        """Generic function to check if a file exists for a release on srrDB with caching"""
        self._clear_old_cache()
        
        # Check cache first
        if release_name in cache_dict:
            return cache_dict[release_name]
        
        try:
            file_url = f"https://www.srrdb.com/download/file/{file_type}/{release_name}.{file_type}"
            req = urllib.request.Request(file_url)
            req.add_header('User-Agent', 'Limnoria SrrDB Plugin/1.1')
            req.get_method = lambda: 'HEAD'  # Only check headers
            
            with urllib.request.urlopen(req, timeout=3) as response:
                exists = response.status == 200
                cache_dict[release_name] = exists
                return exists
        except urllib.error.HTTPError as e:
            exists = e.code != 404
            cache_dict[release_name] = exists
            return exists
        except Exception:
            # Cache negative result for failed checks to avoid repeated attempts
            cache_dict[release_name] = False
            return False

    def _check_sfv_exists(self, release_name):
        """Check if SFV file exists for a release on srrDB with caching"""
        return self._check_file_exists(release_name, 'sfv', self._sfv_cache)

    def _check_m3u_exists(self, release_name):
        """Check if M3U file exists for a release on srrDB with caching"""
        return self._check_file_exists(release_name, 'm3u', self._m3u_cache)

    def _format_size(self, size):
        """Convert size to human readable format"""
        if not isinstance(size, (int, float)) or size <= 0:
            return str(size)
        
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        for unit in units:
            if size < 1024.0:
                if unit == 'B':
                    return f"{int(size)} {unit}"
                else:
                    return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"  # Edge case

    def _build_status_indicators(self, result, has_sfv, has_m3u):
        """Build status indicator string"""
        status_parts = []
        
        # File availability indicators
        if result.get('hasNFO') == "yes":
            status_parts.append(ircutils.mircColor("NFO", 'green'))
        if result.get('hasSRS') == "yes":
            status_parts.append(ircutils.mircColor("SRS", 'green'))
        if has_sfv:
            status_parts.append(ircutils.mircColor("SFV", 'green'))
        if has_m3u:
            status_parts.append(ircutils.mircColor("M3U", 'green'))

        # Confirmation status
        if result.get('confirmed') == "yes":
            status_text = "Status: " + ircutils.mircColor("Confirmed", 'green')
        else:
            status_text = "Status: " + ircutils.mircColor("Unconfirmed", 'red')
        status_parts.append(status_text)
        
        # Special flags
        if result.get('isForeign') == "yes":
            status_parts.append(ircutils.mircColor("Foreign", 'orange'))
        
        return " | ".join(status_parts)

    def _format_result(self, result):
        """Format a single search result"""
        name = result.get('release', 'Unknown')
        size = result.get('size', 'Unknown')
        date = result.get('date', 'Unknown')
        
        # Format size
        formatted_size = self._format_size(size)
        
        # Check file existence
        has_sfv = self._check_sfv_exists(name)
        has_m3u = self._check_m3u_exists(name)
        
        # Build status string
        status_text = self._build_status_indicators(result, has_sfv, has_m3u)
        
        return f"{ircutils.bold(name)} | Size: {formatted_size} | Date: {date} | {status_text}"
    
    def _remove_duplicates(self, results):
        """Remove duplicate results based on release name"""
        unique_results = []
        seen_releases = set()
        
        # Sort to prioritize confirmed releases
        results.sort(key=lambda x: (x.get('release', ''), x.get('confirmed') != 'yes'))
        
        for result in results:
            release_name = result.get('release', '')
            if release_name and release_name not in seen_releases:
                unique_results.append(result)
                seen_releases.add(release_name)
        
        return unique_results
    
    @wrap(['text'])
    def srr(self, irc, msg, args, query):
        """<search term>
        
        Search srrDB for scene releases. Searches both confirmed and unconfirmed releases.
        Usage: srr <release name>
        """
        # Check channel restriction
        channel = msg.channel
        srrdb_enabled = self.registryValue("enabled", channel=channel, network=irc.network)
        if not srrdb_enabled:
            irc.reply("srrdb not enabled in this channel")
            return
                
        if not query.strip():
            irc.reply("Please provide a search term.")
            return
        
        # Perform search
        results = self._search_srrdb(query)
        
        if results is None:
            irc.reply("Error accessing srrDB API. Please try again later.")
            return
        
        if not results:
            irc.reply(f"No releases found for: {query}")
            return
            
        # Process results
        unique_results = self._remove_duplicates(results)
        max_results = min(len(unique_results), 3)
        
        # Output results
        if len(unique_results) == 1:
            irc.reply(f"Found 1 release:")
            irc.reply(self._format_result(unique_results[0]))
        else:
            irc.reply(f"Found {len(unique_results)} releases (showing first {max_results}):")
            for i in range(max_results):
                irc.reply(self._format_result(unique_results[i]))
                
        # Show URL for additional results
        if len(unique_results) > max_results:
            encoded_query = urllib.parse.quote(query)
            irc.reply(f"View all results: https://www.srrdb.com/browse/category:all/search:{encoded_query}")

    @wrap(['text'])
    def srrinfo(self, irc, msg, args, release_name):
        """<release name>
        
        Get detailed information about a specific release from srrDB.
        """
        # Check channel restriction
        channel = msg.channel
        srrdb_enabled = self.registryValue("enabled", channel=channel, network=irc.network)
        if not srrdb_enabled:
            irc.reply("srrdb not enabled in this channel")
            return
        
        if not release_name.strip():
            irc.reply("Please provide a release name.")
            return
        
        # Search for exact release
        results = self._search_srrdb(release_name)
        
        if results is None:
            irc.reply("Error accessing srrDB API.")
            return
        
        # Look for exact match first
        exact_match = None
        for result in results:
            if result.get('release', '').lower() == release_name.lower():
                exact_match = result
                break
        
        if exact_match:
            irc.reply(f"Release info: {self._format_result(exact_match)}")
            
            # Provide download links if files exist
            name = exact_match.get('release', '')
            links = []
            if exact_match.get('hasNFO') == 'yes':
                links.append(f"NFO: https://www.srrdb.com/download/file/nfo/{name}.nfo")
            if exact_match.get('hasSRS') == 'yes':
                links.append(f"SRS: https://www.srrdb.com/download/file/srs/{name}.srs")
            if self._check_sfv_exists(name):
                links.append(f"SFV: https://www.srrdb.com/download/file/sfv/{name}.sfv")
            if self._check_m3u_exists(name):
                links.append(f"M3U: https://www.srrdb.com/download/file/m3u/{name}.m3u")
            
            if links:
                irc.reply("Download links: " + " | ".join(links))
        else:
            irc.reply(f"Exact match not found. Try: srr {release_name}")


Class = SrrDB