import mysql.connector
from mysql.connector import Error, pooling
import time
import queue
import threading
import re
import json
from urllib.parse import quote
from dotenv import load_dotenv
import os
import requests
import supybot.callbacks as callbacks
import supybot.ircmsgs as ircmsgs
import supybot.commands as commands
import supybot.world as world
import contextlib  # For better connection management
from datetime import datetime, date, time as datetime_time
from functools import lru_cache
from supybot.commands import wrap, optional
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock
from collections import OrderedDict
from pathlib import Path
import functools

# Load .env from the plugin directory
plugin_dir = Path(__file__).parent
env_path = plugin_dir / '.env'
load_dotenv(dotenv_path=env_path)

# ====================
# DISCOGS RATE LIMITER
# ====================
class DiscogsRateLimiter:
    """
    Thread-safe rate limiter for Discogs API calls.
    Discogs allows 60 requests/minute for authenticated users.
    Using 25/minute as conservative limit to avoid bursts.
    """
    def __init__(self, calls_per_minute=25):
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.condition = threading.Condition()
    
    def wait_if_needed(self):
        with self.condition:
            now = time.time()
            cutoff = now - 60
            self.calls = [t for t in self.calls if t > cutoff]
            
            if len(self.calls) >= self.calls_per_minute:
                sleep_time = 60 - (now - self.calls[0])
                if sleep_time > 0:
                    self.condition.wait(timeout=sleep_time)
                    now = time.time()
                    cutoff = now - 60
                    self.calls = [t for t in self.calls if t > cutoff]
            
            self.calls.append(time.time())

class PreDBSQL(callbacks.Plugin):
    """Tracks pre database entries and announces them."""

    # Map the section to its colorized string
    section_colors = {
# PRE / SCENENOTiCE
        "PRE": "\00304PRE\003",
        "SPAM": "\00304SPAM\003",        
        "SCENENOTiCE": "\00304SCENENOTiCE\003",
# APPS / 0DAY        
        "APPS": "\00304APPS\003",
        "0DAY": "\003040DAY\003",
# MUSIC
        "MP3": "\00310MP3\003",
        "MP3-WEB": "\00310MP3-WEB\003",
        "FLAC": "\00310FLAC\003",
        "FLAC-WEB": "\00310FLAC\003",
        "FLACFR": "\00310FLAC-FR\003",
        "ABOOK": "\00310ABOOK\003",
        "MVID": "\00310MVID\003",        
        "MViD": "\00310MVID\003",  
        "MDVDR": "\00310MDVDR\003",
        "MBLURAY": "\00310MBLURAY\003",            
# TV
        "TV-UHD-PL": "\00306TV-UHD-PL\003",
        "TV-UHD-DE": "\00306TV-UHD-DE\003",
        "TV-UHD-IT": "\00306TV-UHD-IT\003",
        "TV-UHD-FR": "\00306TV-UHD-FR\003",
        "TV-UHD-CZ": "\00306TV-UHD-CZ\003",
        "TV-UHD-NL": "\00306TV-UHD-NL\003",

        "TV-HD-NL": "\00306TV-HD-NL\003",
        "TV-HD-IT": "\00306TV-HD-IT\003",
        "TV-HD-PL": "\00306TV-HD-PL\003",
        "TV-HD-FR": "\00306TV-HD-FR\003",
        "TV-HD-DE": "\00306TV-HD-DE\003",
        "TV-HD-CZ": "\00306TV-HD-CZ\003",
        "TV-HD-SP": "\00306TV-HD-SP\003",
        "TV-HD-ES": "\00306TV-HD-ES\003",
 
        "TV-SD-NL": "\00306TV-SD-NL\003",
        "TV-SD-IT": "\00306TV-SD-IT\003",
        "TV-SD-PL": "\00306TV-SD-PL\003",
        "TV-SD-FR": "\00306TV-SD-FR\003",
        "TV-SD-DE": "\00306TV-SD-DE\003",
        "TV-SD-CZ": "\00306TV-SD-CZ\003",
        "TV-SD-SP": "\00306TV-SD-SP\003",
        "TV-SD-ES": "\00306TV-SD-ES\003",

        "TV": "\00306TV\003",
        "TV-XVID": "\00306TV-XVID\003",        
        "TV-SDRiP": "\00306TV-SDRiP\003",
        "TV-SD": "\00306TV-SD\003",
        "TV-SD-X264": "\00306TV-SD-X264\003",
        "TV-UHD": "\00306TV-UHD\003",
        "TV-UHDRiP": "\00306TV-UHDRiP\003",
        "TV-HD": "\00306TV-HD\003",
        "TV-HDRiP": "\00306TV-HDRiP\003",
        "TV-HD-NORDiC": "\00306TV-HD-NORDiC\003",
        "TV-UHD-NORDiC": "\00306TV-UHD-NORDiC\003",
# XVID
        "XVID": "\00306XVID\003",

# XXX
        "XXX": "\00313XXX\003",
        "XXX-0DAY": "\00313XXX-0DAY\003",
        "XXX-iMAGESET": "\00313XXX-iMAGESET\003",   
        "XXX-IMAGESET": "\00313XXX-IMAGESET\003",
# DVDR
        "DVDR": "\0035DVDR\003",
        "DVDR-DE": "\0035DVDR-DE\003", 
# X264
        "X264-UHD": "\00302X264-UHD\003",   
        "X264-UHD-NL": "\00302X264-UHD-NL\003",
        "X264-UHD-IT": "\00302X264-UHD-IT\003",  
        "X264-UHD-PL": "\00302X264-UHD-PL\003",
        "X264-UHD-FR": "\00302X264-UHD-FR\003",
        "X264-UHD-DE": "\00302X264-UHD-DE\003",
        "X264-UHD-CZ": "\00302X264-UHD-CZ\003",
        "X264-UHD-ES": "\00302X264-UHD-ES\003",        
        "X264-UHD-SP": "\00302X264-UHD-SP\003",
        "X264-UHD-NORDiC": "\00302X264-UHD-NORDiC\003",  

        "X264": "\00302X264\003",
        "X264-SD": "\00302X264-SD\003",        
        "X264-HD": "\00302X264-HD\003",
        "X264-HD-NL": "\00302X264-HD-NL\003",
        "X264-HD-IT": "\00302X264-HD-IT\003",
        "X264-HD-PL": "\00302X264-HD-PL\003",
        "X264-HD-FR": "\00302X264-HD-FR\003",
        "X264-HD-DE": "\00302X264-HD-DE\003",
        "X264-HD-CZ": "\00302X264-HD-CZ\003",
        "X264-HD-ES": "\00302X264-HD-ES\003",
        "X264-HD-SP": "\00302X264-HD-SP\003",  
        "X264-HD-NORDiC": "\00302X264-HD-NORDiC\003",

        "X264-SD-NL": "\00302X264-SD-NL\003",
        "X264-SD-IT": "\00302X264-SD-IT\003",
        "X264-SD-PL": "\00302X264-SD-PL\003",
        "X264-SD-FR": "\00302X264-SD-FR\003",     
        "X264-SD-DE": "\00302X264-SD-DE\003",
        "X264-SD-CZ": "\00302X264-SD-CZ\003",
        "X264-SD-ES": "\00302X264-SD-ES\003",
#X265
        "X265": "\00311X265\003",
        "X265-NORDiC": "\00311X265-NORDIC\003",
        "X265-HD": "\00311X265-HD\003",
        "X265-NL": "\00311X265-NL\003",
        "X265-IT": "\00311X265-IT\003",        
        "X265-PL": "\00311X265-PL\003",
        "X265-FR": "\00311X265-FR\003",
        "X265-DE": "\00311X265-DE\003",
        "X265-CZ": "\00311X265-CZ\003",
        "X265-ES": "\00311X265-ES\003",
        "X265-SP": "\00311X265-SP\003",        
# BLURAY
        "BLURAY-AVC": "\00312BLURAY\003",
        "BLURAY": "\00312BLURAY\003",
        "BLURAY-UHD": "\00312BLURAY-UHD\003",
        "BLURAY-FULL": "\00312BLURAY\003",
        "BLURAY-FULL-DE": "\00312BLURAY-FULL-DE\003",
        "BLURAY-FULL-SP": "\00312BLURAY-FULL-SP\003",
        "BLURAY-FULL-FR": "\00312BLURAY-FULL-SP\003",
# ANiME
        "ANiME": "\00306ANiME\003",
# SPORT
        "SPORTS": "\00314SPORTS\003",
# GAMES
        "GAMES": "\00307GAMES\003",
        "GAMES-0DAY": "\00307GAMES-0DAY\003",
        "DC": "\00307DC\003",
        "WII": "\00307WII\003",
        "PSX": "\00307PSX\003",  
        "PSV": "\00307PSV\003",        
        "PSP": "\00307PSP\003",  
        "PS2": "\00307PS2\003",              
        "PS3": "\00307PS3\003",
        "PS4": "\00307PS4\003",
        "PS5": "\00307PS5\003",
        "GBA": "\00307GBA\003",
        "GBC": "\00307GBC\003",        
        "NGC": "\00307NGC\003",
        "NDS": "\00307NDS\003",
        "3DS": "\003073DS\003",
        "NSW": "\00307NSW\003",
        "XBOX": "\00307XBOX\003",
        "XBOX360": "\00307XBOX360\003",
        "GAMES-CONSOLE": "\00307GAMES-CONSOLE\003",
        "GAMES-NiNTENDO": "\00307GAMES-NiNTENDO\003",              
# SUBPACK
        "SUBPACK": "\00305SUBPACK\003",
# DOX
        "GAMES-DOX": "\00314GAMES-DOX\003",
        "APPS-MOBiLE": "\00314APPS-MOBiLE\003",
# BOOKWARE / EBOOK
        "BOOKWARE": "\00309BOOKWARE\003",
        "BOOKWARE-0DAY": "\00309BOOKWARE-0DAY\003",        
        "EBOOK": "\00309EBOOK\003",
        "EBOOK-FOREIGN": "\00309EBOOK-FOREIGN\003",
    }

    MUSIC_SECTIONS = {
        'MP3', 'MP3-WEB', 'FLAC', 'FLAC-WEB', 'FLACFR', 
        'FLAC-FR', 'ABOOK', 'MVID', 'MViD', 'MDVDR', 'MBLURAY'
    }

    # ================
    # DECORATORS
    # ================
    @staticmethod
    def admin_only(func):
        """Decorator to restrict commands to admin user only"""
        @functools.wraps(func)  # Preserves function metadata
        def wrapper(self, irc, msg, args, *rest):
            #if msg.nick != 'klapvogn':
            if msg.nick != self.registryValue('adminNick'):
                irc.reply("Error: You do not have permission to use this command.")
                return
            return func(self, irc, msg, args, *rest)
        return wrapper

    # Also add a decorator for multiple authorized users
    @staticmethod
    def authorized_only(allowed_nicks):
        """Decorator for multiple authorized users"""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(self, irc, msg, args, *rest):
                if msg.nick not in allowed_nicks:
                    irc.reply("Error: You do not have permission to use this command.")
                    return
                return func(self, irc, msg, args, *rest)
            return wrapper
        return decorator    
    
    def __init__(self, irc):
        super().__init__(irc)
        self.log = world.log
        # Initialize Discogs rate limiter
        self.discogs_limiter = DiscogsRateLimiter(calls_per_minute=25)        
        self.target_irc_state = None
        self._last_cache_key = None
        self._cached_stats = None
        self.session = requests.Session()
        self._last_memory_check = time.time()
        self._cache_hits = 0
        self._cache_misses = 0     
        self._url_cache = {}  # Manual cache for successful URL shortenings only
        # Pending URL queue for releases that don't exist yet
        self.pending_urls = queue.Queue()
        self.pending_urls_lock = threading.Lock()
        self.pending_urls_cache = {}  # Cache to prevent duplicate queuing
        # Load Discogs API credentials
        self.discogs_token = os.getenv('DISCOGS_TOKEN')
        if self.discogs_token:
            self.log.info("Discogs API token loaded")
        else:
            self.log.warning("DISCOGS_TOKEN not found in environment - Discogs lookups will be disabled")
              
        # iMDB/TMDB URL Fetcher
        # IMDb suggestion API (no key required)
        self.imdb_search_url = "https://v2.sg.media-imdb.com/suggests/{prefix}/{query}.json"
        
        # TMDb API (optional - get key from https://www.themoviedb.org/settings/api)
        # If you don't have a key, IMDb-only works fine for most movies
        self.tmdb_api_key = os.getenv("TMDB_API_KEY", "")
        self.tmdb_search_url = "https://api.themoviedb.org/3/search/movie"
        self.tmdb_tv_search_url = "https://api.themoviedb.org/3/search/tv"

        # Track URL fetch stats
        self.url_fetch_stats = {
            'attempted': 0,
            'succeeded': 0,
            'failed': 0
        }        

        # MySQL configuration        
        self.db_config = {
        'host': os.getenv("MYSQL_HOST"),
        'database': os.getenv("MYSQL_DATABASE"),
        'user': os.getenv("MYSQL_USER"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'port': int(os.getenv("MYSQL_PORT", 3306)),  # Keep default for port since it's usually 3306
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci'
    }
        # Create connection pool
        #self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
        #    pool_name="predb_pool",
        #    pool_size=5,  # Adjust based on your needs
        #    pool_reset_session=True,
        #    **self.db_config
        #)
        self.connection_pool = None
        self._pool_lock = threading.Lock()
        self._pool_retry_after = 0         

        self.nuke_handlers = {
            'nuke': {'type': '1', 'check': '1', 'name': 'nuke', 'pending_cmd': 'newnukes'},
            'unnuke': {'type': '2', 'check': '2', 'name': 'unnuke', 'pending_cmd': 'newunnukes'},
            'modnuke': {'type': '3', 'check': '3', 'name': 'modnuke', 'pending_cmd': 'newmodnukes'},
            'delpre': {'type': '4', 'check': '4', 'name': 'delpre', 'pending_cmd': 'newdelpres'},
            'undelpre': {'type': '5', 'check': '5', 'name': 'undelpre', 'pending_cmd': 'newundelpres'},
        }         

        # Load pending URLs from database on startup
        self._load_pending_urls_from_db()        

        # Start pending URL processor thread
        self.pending_processor_thread = threading.Thread(
            target=self._process_pending_urls,
            daemon=True,
            name="pending_urls_processor"
        )
        self.pending_processor_thread.start()

        self.url_stats = {
            'immediate_success': 0,
            'queued': 0,
            'delayed_success': 0,
            'failed': 0
        }                    

        # Main thread pool for IRC command handling
        self.thread_pool = ThreadPoolExecutor(
            max_workers=3,
            thread_name_prefix='predb_worker'
        )        
        # Separate pool ONLY for external HTTP link lookups (NFO/SFV/SRR)
        # Must be separate to avoid deadlock when called from within thread_pool workers
        self.link_pool = ThreadPoolExecutor(
            max_workers=6,
            thread_name_prefix='predb_links'
        )        

        # Track active tasks
        self._active_tasks = 0
        self._max_active_tasks = 0        
        
        # Initialize caches for HTTP responses
        self.cache_maxsize = 50
        self.nfo_cache = OrderedDict()
        self.sfv_cache = OrderedDict()
        self.srr_cache = OrderedDict()
               
        # Thread-safe cache locks
        self.nfo_cache_lock = threading.Lock()
        self.sfv_cache_lock = threading.Lock()
        self.srr_cache_lock = threading.Lock()

        # Cache for release name matching to avoid repeated DB lookups
        self.release_cache = {}
        self.cache_ttl = 300  # 5 minutes
        self.cache_timestamps = {}

        # Command handlers mapping - ADD ALL YOUR COMMANDS HERE
        self.command_handlers = {
            "!addpre": self.handle_addpre,
            "!gn": self.handle_addgenre,
            "!addurl": self.handle_addurl,
            "!info": self.handle_addinfo,
            "!nuke": self.handle_addnuke,
            "!unnuke": self.handle_addunnuke,
            "!modnuke": self.handle_addmodnuke,
            "!delpre": self.handle_adddelpre,
            "!undelpre": self.handle_addundelpre,
        }

    def discogs(self, irc, msg, args, releasename):
        """<releasename> - Search Discogs for a music release and update the URL in the database
        
        Searches Discogs API for the specified music release and stores the URL in your predb.
        Requires DISCOGS_TOKEN in .env file.
        
        Usage: +discogs <releasename>
        Example: +discogs Wreckless-Unforced_Rhythms-(DISWRLP001BP)-16BIT-WEB-FLAC-2026-PTC
        """
        if not releasename:
            irc.reply("Usage: +discogs <releasename>")
            return
        
        # Check if Discogs token is configured
        if not getattr(self, 'discogs_token', None):
            irc.reply("⚠ Discogs API token not configured. Add DISCOGS_TOKEN to .env file.")
            return
        
        # Check if release exists in database
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT section, url FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                result = cursor.fetchone()
                
                if not result:
                    irc.reply(f"Release '{releasename}' not found in database")
                    return
                
                section, current_url = result
                
                if section not in self.MUSIC_SECTIONS:
                    irc.reply(f"Release is in section '{section}', not a music section")
                    return
                
                if current_url:
                    irc.reply(f"Release already has URL: {current_url} - Searching anyway...")
        
        except Exception as e:
            self.log.error(f"Error checking release: {e}")
            irc.reply("Database error while checking release")
            return
        
        irc.reply(f"🔍 Searching Discogs API for: {releasename}")
        
        # Run in thread to avoid blocking IRC
        def lookup_thread():
            # PASS IRC for announcements
            url, message = self._process_discogs_lookup(releasename, announce_irc=irc)
            if url:
                irc.reply(f"✓ {message}")
                # announce_url() already called in _process_discogs_lookup
            else:
                irc.reply(f"✗ {message}")
        
        thread = Thread(target=lookup_thread, daemon=True)
        thread.start()

    discogs = wrap(discogs, ['text'])

    def autodiscogs(self, irc, msg, args, enable_disable):
        """<on|off> - Enable or disable automatic Discogs lookup for new music releases
        
        When enabled, the bot will automatically search Discogs and update URLs
        for all new music releases as they are announced.
        
        Usage: +autodiscogs on|off
        """
        if not enable_disable or enable_disable.lower() not in ['on', 'off']:
            current_status = "enabled" if self.registryValue('autoDiscogs') else "disabled"
            irc.reply(f"Automatic Discogs lookup is currently {current_status}. Usage: +autodiscogs <on|off>")
            return
        
        new_value = (enable_disable.lower() == 'on')
        self.setRegistryValue('autoDiscogs', new_value)

        status = "enabled" if new_value else "disabled"
        irc.reply(f"✓ Automatic Discogs lookup {status}")
        self.log.info(f"Automatic Discogs lookup {status}")

    autodiscogs = wrap(autodiscogs, ['text'])    

    # ==========================================
    # AUTO URL FETCHER METHODS FOR iMDB AND TMDB
    # ==========================================
    def parse_release_name(self, releasename):
        """Extract clean title, year, and type from scene release name"""
        # Remove group at the end
        main = releasename.rsplit('-', 1)[0] if '-' in releasename else releasename
        
        # TV patterns: Show.Name.S01E02 or Show.Name.01x02
        tv_patterns = [
            (r'^(.*?)\.[Ss](\d{1,2})[Ee](\d{1,2})', 'tv_season_episode'),
            (r'^(.*?)\.(\d{1,2})x(\d{1,2})', 'tv_season_episode'),
            (r'^(.*?)\.S(\d{1,2})D(\d{1,2})', 'tv_season_disc'),  # S01D01 format
            (r'^(.*?)\.Complete\.S(\d{1,2})', 'tv_complete_season'),
            (r'^(.*?)\.Season\.(\d{1,2})', 'tv_season'),
        ]
        
        for pattern, tv_type in tv_patterns:
            match = re.match(pattern, main, re.IGNORECASE)
            if match:
                title = match.group(1).replace('.', ' ').strip()
                season = match.group(2)
                return {
                    'type': 'tv',
                    'title': title,
                    'season': season,
                    'year': None,
                    'tv_type': tv_type
                }
        
        # Movie pattern: Title.2024.1080p... or Title.2024.BluRay...
        movie_match = re.match(r'^(.*?)\.(\d{4})\.', main)
        if movie_match:
            title = movie_match.group(1).replace('.', ' ').strip()
            year = movie_match.group(2)
            return {
                'type': 'movie',
                'title': title,
                'year': year,
                'season': None
            }
        
        # Fallback: just clean up dots, no year found
        title = main.split('.')[0].replace('.', ' ').strip()
        return {
            'type': 'unknown',
            'title': title,
            'year': None,
            'season': None
        }

    def fetch_imdb_url(self, title, year=None):
        """Fetch IMDb URL using IMDb's suggestion API (no API key needed)"""
        try:
            # Clean title for IMDb search
            search_title = re.sub(r'[^\w\s]', '', title).lower().strip()
            if not search_title:
                return None
                
            prefix = search_title[0] if search_title[0].isalnum() else 't'
            query = quote(search_title.replace(' ', '_'))
            
            url = self.imdb_search_url.format(prefix=prefix, query=query)
            response = self.session.get(url, timeout=5)
            
            if response.status_code != 200:
                return None
            
            # Parse JSONP format: imdb$title(data)
            text = response.text
            if '(' not in text or not text.endswith(')'):
                return None
                
            # Extract JSON from JSONP wrapper
            json_str = text[text.find('(')+1:-1]
            data = json.loads(json_str)
            
            results = data.get('d', [])
            if not results:
                return None
            
            # Try to match by year first if provided
            if year:
                for item in results:
                    item_year = str(item.get('y', ''))
                    if item_year == year:
                        imdb_id = item.get('id')
                        if imdb_id and imdb_id.startswith('tt'):
                            return f"https://www.imdb.com/title/{imdb_id}/"
            
            # Return first result if no year match or no year provided
            imdb_id = results[0].get('id')
            if imdb_id and imdb_id.startswith('tt'):
                return f"https://www.imdb.com/title/{imdb_id}/"
                
        except Exception as e:
            self.log.debug(f"IMDb fetch error for '{title}': {e}")
        return None

    def fetch_tmdb_movie_url(self, title, year=None):
        """Fetch TMDb movie URL (requires API key)"""
        if not self.tmdb_api_key:
            return None
            
        try:
            params = {
                'api_key': self.tmdb_api_key,
                'query': title,
                'page': 1
            }
            if year:
                params['year'] = year
                
            response = self.session.get(self.tmdb_search_url, params=params, timeout=5)
            data = response.json()
            
            if data.get('results'):
                movie_id = data['results'][0]['id']
                return f"https://www.themoviedb.org/movie/{movie_id}"
                
        except Exception as e:
            self.log.debug(f"TMDb movie fetch error for '{title}': {e}")
        return None

    def fetch_tmdb_tv_url(self, title, season=None):
        """Fetch TMDb TV URL (requires API key)"""
        if not self.tmdb_api_key:
            return None
            
        try:
            params = {
                'api_key': self.tmdb_api_key,
                'query': title,
                'page': 1
            }
            
            response = self.session.get(self.tmdb_tv_search_url, params=params, timeout=5)
            data = response.json()
            
            if data.get('results'):
                tv_id = data['results'][0]['id']
                return f"https://www.themoviedb.org/tv/{tv_id}"
                
        except Exception as e:
            self.log.debug(f"TMDb TV fetch error for '{title}': {e}")
        return None

    def auto_fetch_url(self, releasename):
        """Auto-fetch URL for a release name - tries multiple sources"""
        self.url_fetch_stats['attempted'] += 1
        
        parsed = self.parse_release_name(releasename)
        url = None
        
        if parsed['type'] == 'movie':
            # Try IMDb first (no key needed)
            url = self.fetch_imdb_url(parsed['title'], parsed.get('year'))
            
            # Fallback to TMDb if available and IMDb failed
            if not url and self.tmdb_api_key:
                url = self.fetch_tmdb_movie_url(parsed['title'], parsed.get('year'))
        
        elif parsed['type'] == 'tv':
            # For TV, prefer TMDb if available (better TV database)
            if self.tmdb_api_key:
                url = self.fetch_tmdb_tv_url(parsed['title'], parsed.get('season'))
            
            # Fallback to IMDb TV search (treat as movie search, often works)
            if not url:
                url = self.fetch_imdb_url(parsed['title'])
        
        else:
            # Unknown type - try IMDb anyway
            url = self.fetch_imdb_url(parsed['title'])
        
        if url:
            self.url_fetch_stats['succeeded'] += 1
            self.log.info(f"✓ Found URL for {releasename}: {url}")
        else:
            self.url_fetch_stats['failed'] += 1
            self.log.debug(f"✗ No URL found for {releasename}")
        
        return url

    def _background_url_fetch(self, releasename):
        """Background task to fetch and update URL after release is announced"""
        try:
            # Small delay to not overwhelm APIs
            time.sleep(2)
            
            ### ADD THIS BLOCK START ###
            # Check if this is a music release - skip URL fetching for music
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT section FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                section_result = cursor.fetchone()
                if section_result:
                    section = section_result[0]
                    skip_sections = {
                        # MUSIC
                        'MP3', 'MP3-WEB', 'FLAC', 'FLAC-WEB', 'FLACFR', 
                        'FLAC-FR', 'ABOOK', 'MVID', 'MViD', 'MDVDR', 'MBLURAY',
                        # GAMES
                        'GAMES', 'GAMES-0DAY', 'DC', 'WII', 'PSX', 'PSV', 'PSP',
                        'PS2', 'PS3', 'PS4', 'PS5', 'GBA', 'GBC', 'NGC', 'NDS', '3DS',
                        'NSW', 'XBOX', 'XBOX360', 'GAMES-CONSOLE', 'GAMES-NiNTENDO',
                        # EBOOK
                        'EBOOK', 'BOOKWARE', 'BOOKWARE-0DAY', 'EBOOK-FOREIGN',
                        # APPS / VARIOUS
                        'APPS', '0DAY', 'SPORTS', 'GAMES-DOX', 'APPS-MOBiLE',
                        # XXX
                        'XXX'
                    }                             
                    if section in skip_sections:
                        self.log.info(f"Skipping auto URL fetch for {section} release: {releasename}")
                        return
            ### ADD THIS BLOCK END ###
            
            url = self.auto_fetch_url(releasename)
            
            if not url:
                return  # No URL found, nothing to update
            
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Double-check URL is still empty before updating
                cursor.execute(
                    "SELECT url FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                result = cursor.fetchone()
                
                if result:
                    current_url = result[0]
                    # Only update if empty or NULL
                    if not current_url:
                        cursor.execute(
                            "UPDATE releases SET url = %s WHERE releasename = %s",
                            (url, releasename),
                        )
                        conn.commit()
                        self.log.info(f"✓ Background URL updated for {releasename}")
                        
                        # ANNOUNCE URL WAS ADDED (reusing your existing method)
                        # Need to get irc object - try to find it from world.ircs
                        for irc in world.ircs:
                            if self._is_irc_connected(irc):
                                self.announce_url(irc, releasename, url)
                                break
                        
                    else:
                        self.log.debug(f"URL already exists for {releasename}, skipping")
                        
        except Exception as e:
            self.log.error(f"Background URL fetch error for {releasename}: {e}")

    # ==============
    # DISCOGS LOOKUP
    # ==============
    def _background_discogs_lookup(self, releasename, irc=None):
        """Background worker for Discogs URL lookup (runs in thread pool)"""
        try:
            self.log.info(f"Starting Discogs lookup for: {releasename}")
            
            # Pass IRC for announcements
            url, message = self._process_discogs_lookup(releasename, announce_irc=irc)
            
            if url:
                self.log.info(f"✓ Auto-Discogs SUCCESS: {message} - {url}")
            else:
                self.log.warning(f"✗ Auto-Discogs FAILED: {message}")
        except Exception as e:
            self.log.error(f"Error in auto Discogs lookup for {releasename}: {e}", exc_info=True)         

    # =================
    # URL FETCHER STATS
    # =================
    def urlfetchstats(self, irc, msg, args):
            """Show URL fetcher statistics"""
            stats = self.url_fetch_stats
            total = stats['attempted']
            if total > 0:
                success_rate = (stats['succeeded'] / total) * 100
                irc.reply(
                    f"[ URL Fetcher Stats ] "
                    f"[ Attempted: {stats['attempted']} ] "
                    f"[ Succeeded: \x0303{stats['succeeded']}\x03 ] "
                    f"[ Failed: \x0304{stats['failed']}\x03 ] "
                    f"[ Success Rate: {success_rate:.1f}% ]"
                )
            else:
                irc.reply("[ URL Fetcher Stats ] No fetches attempted yet")

    urlfetchstats = commands.wrap(urlfetchstats, [])

    # ====================
    # FETCHER MISSING URLS
    # ====================
    def fetchmissingurls(self, irc, msg, args, limit=None):
        """[<limit>] - Manually fetch URLs for releases without them"""
        if not limit:
            limit = 10
        limit = min(max(limit, 1), 50)  # Clamp between 1-50
        
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT releasename FROM releases 
                    WHERE url IS NULL OR url = ''
                    ORDER BY unixtime DESC
                    LIMIT %s
                """, (limit,))
                
                releases = cursor.fetchall()
                
                if not releases:
                    irc.reply("No releases missing URLs")
                    return
                
                irc.reply(f"Fetching URLs for {len(releases)} releases...")
                
                # Submit all to thread pool
                for (releasename,) in releases:
                    self.thread_pool.submit(self._background_url_fetch, releasename)
                    time.sleep(0.5)  # Small delay between submissions
                
                irc.reply(f"Queued {len(releases)} URL fetches (background processing)")
                
        except Exception as e:
            self.log.error(f"Fetch missing URLs error: {e}")
            irc.reply(f"Error: {e}")

    fetchmissingurls = commands.wrap(fetchmissingurls, [optional('int')])    

    def _process_command(self, text):
        """Process commands efficiently"""
        # Logging command from above
        #self.log.info(f"Processing command: {text}")
        
        for cmd_prefix, handler in self.command_handlers.items():
            if text.startswith(cmd_prefix):
                args = text.split()[1:]
                #self.log.info(f"{cmd_prefix} args: {args}, length: {len(args)}")
                
                # Check arg count based on command requirements
                min_args = 3 if cmd_prefix in ["!info", "!nuke", "!delpre", "!undelpre", "!modnuke", "!unnuke"] else 2
                
                #self.log.info(f"Required args: {min_args}, Got: {len(args)}")
                
                if len(args) >= min_args:
                    #self.log.info(f"Calling handler for {cmd_prefix}")
                    handler(self.irc, self.msg, args)
                else:
                    self.log.info(f"Not enough args for {cmd_prefix}: {len(args)}")
                return True
        
        self.log.info("No command handler matched")
        return False

    @contextlib.contextmanager
    def db_connection(self):
        """Get connection from pool, creating pool lazily if needed"""
        conn = None
        try:
            # Lazy pool creation with retry backoff
            if self.connection_pool is None:
                with self._pool_lock:
                    if self.connection_pool is None:  # double-checked locking
                        now = time.time()
                        if now < self._pool_retry_after:
                            raise mysql.connector.Error(
                                f"MySQL unavailable, retrying after {int(self._pool_retry_after - now)}s"
                            )
                        try:
                            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                                pool_name="predb_pool",
                                pool_size=5,
                                pool_reset_session=True,
                                **self.db_config
                            )
                            self.log.info("MySQL connection pool created successfully")
                        except mysql.connector.Error as e:
                            self._pool_retry_after = time.time() + 30  # retry in 30s
                            self.log.error(f"Failed to create MySQL pool: {e} — retrying in 30s")
                            raise

            conn = self.connection_pool.get_connection()
            yield conn
        except mysql.connector.Error as e:
            self.log.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
                self.log.debug("Database connection closed")

    # ========================
    # CACHED HTTP REQUESTS
    # ========================
    @lru_cache(maxsize=100)
    def get_cached_content(self, url):
        """Cached HTTP GET with proper size enforcement and thread-safety"""
        with self.nfo_cache_lock:
            if url in self.nfo_cache:
                # Move to end (most recently used)
                content = self.nfo_cache.pop(url)
                self.nfo_cache[url] = content
                return content
        
        try:
            response = self.session.get(url, timeout=5)
            content = response.text if response.status_code == 200 else None
            
            with self.nfo_cache_lock:
                # Enforce cache size limit
                if len(self.nfo_cache) >= self.cache_maxsize:
                    self.nfo_cache.popitem(last=False)  # Remove oldest
                
                self.nfo_cache[url] = content
            return content
        except Exception:
            return None

    def get_nfo_sfv_from_srrdb(self, releasename):
        """Single API call returning both NFO and SFV links"""
        url = f"https://api.srrdb.com/v1/nfo/{releasename}"
        content = self.get_cached_content(url)
        
        if content:
            try:
                data = json.loads(content)
                if data.get('nfolink'):
                    nfo_url = data['nfolink'][0]
                    sfv_url = nfo_url.replace('.nfo', '.sfv')
                    return (
                        f"[ \x033NFO\x03: {self.shorten_url(nfo_url)} ] ",
                        f"[ \x033SFV\x03: {self.shorten_url(sfv_url)} ] "
                    )
            except (json.JSONDecodeError, KeyError, IndexError):
                pass
        return "[ \x0305NFO\x03 ]", "[ \x0305SFV\x03 ]"

    def get_srr_from_srrdb(self, releasename):
        """Cached SRR lookup with timeout - checks if SRR download exists"""
        url = f"https://www.srrdb.com/download/srr/{releasename}"
        
        # Check cache first (thread-safe)
        with self.srr_cache_lock:
            if url in self.srr_cache:
                # Move to end (most recently used)
                exists = self.srr_cache.pop(url)
                self.srr_cache[url] = exists
                if exists:
                    shortened = self.shorten_url(url)
                    return f"[ \x033SRR\x03: {shortened} ] "
                return f"[ \x0305SRR\x03 ]"
        
        # Make HEAD request to check if SRR exists without downloading
        try:
            response = self.session.head(url, timeout=5, allow_redirects=True)
            exists = response.status_code == 200
            
            with self.srr_cache_lock:
                # Enforce cache size limit
                if len(self.srr_cache) >= self.cache_maxsize:
                    self.srr_cache.popitem(last=False)  # Remove oldest
                
                self.srr_cache[url] = exists
            
            if exists:
                shortened = self.shorten_url(url)
                return f"[ \x033SRR\x03: {shortened} ] "
            return f"[ \x0305SRR\x03 ]"
            
        except Exception as e:
            self.log.error(f"Error checking SRR: {e}")
            return f"[ \x0305SRR\x03 ]"
        
    def get_all_links(self, releasename):
        """Get NFO, SFV, and SRR synchronously - called from within a thread pool worker"""
        nfo_text, sfv_text = self.get_nfo_sfv_from_srrdb(releasename)
        srr_text = self.get_srr_from_srrdb(releasename)
        return nfo_text, sfv_text, srr_text

    # ========================
    # URL SHORTENING
    # ========================
    def shorten_url(self, long_url):
            """
            URL shortening with selective caching.
            
            Only caches successfully shortened URLs. Failed attempts return the long URL
            but are NOT cached, allowing retry on subsequent calls.
            
            Args:
                long_url: The full URL to shorten
                
            Returns:
                Shortened TinyURL if successful, otherwise the original long URL
            """
            # Check manual cache first - only contains successful shortenings
            if long_url in self._url_cache:
                return self._url_cache[long_url]
            
            try:
                tinyurl_api = f"https://tinyurl.com/api-create.php?url={long_url}"
                # Increased timeout from 3 to 5 seconds for more reliability
                response = self.session.get(tinyurl_api, timeout=5)
                
                # Validate response is a valid HTTP URL
                if response.status_code == 200 and response.text.startswith('http'):
                    shortened = response.text.strip()
                    
                    # Verify it's actually a TinyURL before caching
                    if 'tinyurl.com' in shortened and shortened != long_url:
                        # Cache ONLY successful shortenings
                        self._url_cache[long_url] = shortened
                        
                        # Enforce cache size limit (keep last 100 successful shortenings)
                        if len(self._url_cache) > 100:
                            # Remove oldest entry (first item in dict)
                            oldest_key = next(iter(self._url_cache))
                            self._url_cache.pop(oldest_key)
                        
                        return shortened
                
                # Shortening failed - log and return long URL (NOT cached)
                self.log.warning(f"URL shortening failed for {long_url}: status={response.status_code}")
                return long_url
                
            except Exception as e:
                # Exception during shortening - log and return long URL (NOT cached)
                self.log.warning(f"URL shortening exception for {long_url}: {e}")
                return long_url
        
    # ========================
    # TIME FORMATTING
    # ========================
    def format_time_ago(self, timestamp):
        """
        Format a timestamp as a human-readable time difference with IRC color coding.
        
        Converts a Unix timestamp into a string representing how long ago it occurred,
        with appropriate IRC color codes based on the age of the timestamp. For older
        timestamps (over 30 days), provides a detailed breakdown including years,
        months, days, hours, minutes, and seconds.
        
        Parameters
        ----------
        timestamp : float
            Unix timestamp (seconds since epoch) to format as time difference
            
        Returns
        -------
        tuple
            A tuple containing:
            - time_str (str): Human-readable time difference (e.g., "2 days 3 hours ago")
            - color (str): IRC color code string for the time difference
            
        Examples
        --------
        >>> format_time_ago(time.time() - 300)  # 5 minutes ago
        ('5 minutes ago', '\\x0303')
        
        >>> format_time_ago(time.time() - 2592000)  # 30 days ago  
        ('1 month ago', '\\x0313')
        
        Notes
        -----
        Color coding:
        - < 1 hour: Green (\\x0303)
        - < 1 day: Orange (\\x0307) 
        - < 1 week: Yellow (\\x0308)
        - < 1 month: Red (\\x0304)
        - ≥ 1 month: Pink (\\x0313)
        
        For periods over 30 days, the output includes all significant time components
        down to seconds, ensuring precise representation of long time differences.
        """
        now = time.time()
        diff = int(now - timestamp)
        
        # Choose color based on age (older = different color)
        if diff < 3600:  # Less than 1 hour
            color = "\x0303"  # Green
        elif diff < 86400:  # Less than 1 day
            color = "\x0307"  # Orange
        elif diff < 604800:  # Less than 1 week
            color = "\x0308"  # Yellow
        elif diff < 2592000:  # Less than 1 month
            color = "\x0304"  # Red
        else:  # Older than 1 month
            color = "\x0313"  # Pink
        
        if diff < 60:
            time_str = f"{diff} second{'s' if diff != 1 else ''} ago"
        elif diff < 3600:
            minutes = diff // 60
            time_str = f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif diff < 86400:
            hours = diff // 3600
            time_str = f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif diff < 2592000:  # 30 days
            days = diff // 86400
            time_str = f"{days} day{'s' if days != 1 else ''} ago"
        else:
            # For periods longer than a month, show detailed breakdown
            total_seconds = diff
            
            years = total_seconds // (365 * 24 * 3600)
            remaining_seconds = total_seconds % (365 * 24 * 3600)
            
            months = remaining_seconds // (30 * 24 * 3600)
            remaining_seconds %= (30 * 24 * 3600)
            
            days = remaining_seconds // (24 * 3600)
            remaining_seconds %= (24 * 3600)
            
            hours = remaining_seconds // 3600
            remaining_seconds %= 3600
            
            minutes = remaining_seconds // 60
            seconds = remaining_seconds % 60
            
            # Build formatted string
            components = []
            if years > 0:
                components.append(f"{years} year{'s' if years != 1 else ''}")
            if months > 0:
                components.append(f"{months} month{'s' if months != 1 else ''}")
            if days > 0:
                components.append(f"{days} day{'s' if days != 1 else ''}")
            if hours > 0:
                components.append(f"{hours} hour{'s' if hours != 1 else ''}")
            if minutes > 0:
                components.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
            if seconds > 0 or not components:
                components.append(f"{seconds} second{'s' if seconds != 1 else ''}")
            
            time_str = ' '.join(components) + ' ago'
        
        return time_str, color

    # ================
    # Pre Search Cache
    # ================    
    @lru_cache(maxsize=100)  # Caches the last 100 queries to improve performance
    def fetch_release(self, release):
        with self.db_connection() as conn:
            cursor = conn.cursor()
            if release == "*":
                cursor.execute("""
                    SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet 
                    FROM releases 
                    ORDER BY unixtime DESC 
                    LIMIT 1;
                """)
            else:
                cursor.execute("""
                    SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet 
                    FROM releases 
                    WHERE releasename = %s
                    LIMIT 1;
                """, (release,))
            return cursor.fetchone()
    # End

    # ===============
    # CHANGE UNIXTIME
    # ===============
    @admin_only
    def unixtime(self, irc, msg, args):
        """Update the unixtime for a specific release in the database.
        
        This command allows authorized users to modify the unixtime timestamp
        associated with a release entry in the encrypted database.
        
        Security:
            - Only the user 'klapvogn' is authorized to execute this command
            - All database operations are performed through an encrypted SQLCipher connection
        
        Args:
            irc: IRC connection object for sending replies
            msg: Message object containing command context and user information
            args: List of command arguments [releasename, unixtime]
        
        Usage:
            +unixtime <releasename> <unixtime>
        
        Examples:
            +unixtime "My Release Name" 1633046400
            +unixtime project-alpha 1633132800
        
        Validation:
            - Verifies user authorization
            - Validates argument count (exactly 2 arguments required)
            - Ensures unixtime is a valid integer
        
        Database Operations:
            - Updates the 'unixtime' field in the 'releases' table
            - Uses parameterized queries to prevent SQL injection
            - Commits transaction only if the update affects exactly one row
            - Returns appropriate feedback based on whether the release was found
        
        Error Handling:
            - Permission denied for unauthorized users
            - Invalid argument count or format
            - Database connection/query errors
            - Unexpected exceptions with proper logging
        
        Returns:
            None - sends replies directly via IRC connection
        """
        if len(args) < 2:
            irc.reply("Usage: +unixtime <releasename> <unixtime>")
            return

        releasename, new_unixtime = args[0], args[1]

        # Ensure the provided unixtime is a valid integer
        try:
            new_unixtime = int(new_unixtime)
        except ValueError:
            irc.reply("Error: unixtime must be a valid integer.")
            return

        try:
            # Connect to the MySQL database
            with self.db_connection() as conn:
                # Execute the update query
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET unixtime = %s WHERE releasename = %s",
                    (new_unixtime, releasename)
                )

                # Check if any rows were affected
                if cursor.rowcount > 0:
                    conn.commit()
                    irc.reply(f"Updated unixtime for release: {releasename} to {new_unixtime}.")
                else:
                    irc.reply(f"Release {releasename} not found in the database.")
        except Error as e:
            self.log.error(f"MySQL database error during unixtime change: {e}")
            irc.reply(f"Error during unixtime change: {e}")
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # ==============
    # CHANGE SECTION
    # ==============  
    @admin_only
    def chgsec(self, irc, msg, args):
        """Update the section for a specific release in the database.
        
        This command allows authorized users to modify the section assignment
        for an existing release. The command is restricted to the user 'klapvogn'
        for security reasons.
        
        Usage: !chgsec <releasename> <new_section>
        
        Args:
            irc: IRC connection object for sending responses
            msg: Message object containing command context and sender info
            args: List of command arguments [releasename, new_section]
        
        Security:
            - Only the user 'klapvogn' is authorized to execute this command
        
        Process:
            1. Verify user authorization
            2. Validate argument count
            3. Update section in MySQL database
            4. Provide appropriate success/error feedback
        
        Raises:
            Error: If MySQL database operations fail
            Exception: For any other unexpected errors
        
        Examples:
            !chgsec "MyRelease v1.0" "stable"
            !chgsec "TestBuild" "beta"
        
        Notes:
            - All errors are logged for debugging purposes
            - Database changes are committed only if the update affects rows
            - The release name must match exactly (case-sensitive)
        """
        if len(args) < 2:
            irc.reply("Usage: !chgsec <releasename> <new_section>")
            return

        releasename, new_section = args[0], args[1]

        try:
            # Connect to the MySQL database
            with self.db_connection() as conn:
                
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET section = %s WHERE releasename = %s",
                    (new_section, releasename)
                )
                if cursor.rowcount > 0:
                    conn.commit()
                    irc.reply(f"Updated section for release: {releasename} to {new_section}.")
                else:
                    irc.reply(f"Release {releasename} not found in the database.")
        except Error as e:
            self.log.error(f"MySQL database error during section change: {e}")
            irc.reply(f"Error during section change: {e}")
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # ===
    # Pre
    # ===  
    def pre(self, irc, msg, args, release):
        """Fetch pre-release information from the database.
        
        Retrieves and displays detailed information about scene pre-releases with
        formatted output including timing, file details, and nuke status.
        
        Args:
            irc: IRC connection object
            msg: Message object containing command context
            args: Command arguments (processed by wrap decorator)
            release: Release name to search for, or "*" for latest release
        
        Behavior:
            - With specific release: Searches for exact match of release name
            - With "*": Returns the most recent release by timestamp
            - Formats output with IRC colors and timing information
            - Provides file links (NFO, SFV, SRR) when available
            - Displays nuke status and reasons if applicable
        
        Examples:
            !pre Some.Movie.2024.1080p.BLURAY
            !pre *
        
        Response Format:
            [ PRED ] [ Release.Name ] [ TIME: 2 hours ago / 2024-01-15 14:30:00 GMT ] 
            in [ SECTION / GENRE ] [ INFO: 1500 MB, 5 Files ] [ Nuked: Reason => Source ]
            [NFO] [SFV] [SRR]
        
        Notes:
            - Uses optimized database queries for performance
            - Handles both nuked and unnuked releases
            - Provides human-readable time formatting
            - Returns appropriate error messages for missing releases
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                if release == "*":
                    cursor.execute("""
                        SELECT releasename, section, unixtime, files, size, grp, genre, url, nuked, reason, nukenet 
                        FROM releases 
                        ORDER BY unixtime DESC 
                        LIMIT 1
                    """)
                else:
                    cursor.execute("""
                        SELECT releasename, section, unixtime, files, size, grp, genre, url, nuked, reason, nukenet 
                        FROM releases 
                        WHERE releasename = %s
                        LIMIT 1
                    """, (release,))
                result = cursor.fetchone()
                
            if not result:
                # Single error message for both cases
                irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            # Unpack and process result
            releasename, section, unixtime, files, size, grp, genre, url, nuked, reason, nukenet = result
            # Lookup the section color
            section_formatted = self.section_colors.get(section, section)  # Default to section name if not found 
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"

            # Get URL display
            if url:
                url_text = f"[ \x033URL\x03: {url} ] "
            else:
                url_text = f"[ \x0305URL\x03 ]"
            
            # With parallel execution using link_pool:
            nfo_sfv_future = self.link_pool.submit(self.get_nfo_sfv_from_srrdb, releasename)
            srr_future = self.link_pool.submit(self.get_srr_from_srrdb, releasename)
            nfo_text, sfv_text = nfo_sfv_future.result()
            srr_text = srr_future.result()
            # Info and section        
            info_string = f"[ \x033INFO\x03: {size} MB, {files} Files ] " if size and files else ""
            section_and_genre = f"[ {section_formatted} / {genre} ]" if genre and genre.lower() != 'null' else f"[ {section_formatted} ]"

            nuke_status = {
                '1': ("\x0304Nuked\x03", "\x0304"),
                '2': ("\x0303UnNuked\x03", "\x0303"),
                '3': ("\x0305ModNuked\x03", "\x0305"),
                '4': ("\x0305DelPred\x03", "\x0305"),
                '5': ("\x0303UnDelPred\x03", "\x0303")
            }
            status, color = nuke_status.get(nuked, ("", ""))
            nuked_details = f"[ {status}: {color}{reason or 'No reason'}\x03 => {color}{nukenet or 'Unknown'}\x03 ]" if status else ""
                     
            # Build response
            message = (
                f"\x033[ PRED ]\x03 [ {releasename} ] [ \x033TIME\x03: {time_ago} / {pretime_colored} ] "
                f"in {section_and_genre} {info_string}{nuked_details}"
                f"{url_text}{nfo_text}{sfv_text}{srr_text}"
            )
            irc.reply(message)

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")
    pre = commands.wrap(pre, ['text'])

    # ====
    # DUPE
    # ====
    def dupe(self, irc, msg, args, release):
        """Search for duplicate releases in the database.
        
        This command searches the pre database for releases that match the given
        search term and returns up to 10 most recent results with detailed
        information including release metadata, nuke status, and file links.
        
        Parameters
        ----------
        release : str
            The search term for the release name. Supports wildcard matching
            where '%' is converted to '*' for SQL LIKE pattern matching.
            Search is case-insensitive.
        
        Returns
        -------
        None
            Results are sent via private message to the user. If no results
            are found, an error message is displayed in the channel.
        
        Examples
        --------
        <botname> dupe Some.Movie.2024
            Searches for releases starting with "Some.Movie.2024"
        
        <botname> dupe Some%Movie%
            Searches for releases containing "Some" and "Movie" with wildcards
        
        Notes
        -----
        - Returns up to 10 most recent matches ordered by timestamp
        - Search uses SQL LIKE pattern matching with the term as prefix
        - Results include: release name, section, time, file info, nuke status
        - Provides download links for NFO, SFV, and SRR files when available
        - All results are sent via private message to avoid channel spam
        """
        # Clean and format the release input
        sea1 = release.replace("%", "*").strip()
        sea1 = sea1.lower()  # Normalize to lowercase for case-insensitive matching

        # Strip any leading wildcards to protect index usage
        sea1 = sea1.lstrip('*%')
        if not sea1:
            irc.reply("Search term too vague, please be more specific.")
            return
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet
                    FROM releases
                    WHERE releasename LIKE %s
                    ORDER BY unixtime DESC
                    LIMIT 10
                """, (f"{sea1}%",))
                results = cursor.fetchall()

            if not results:
                irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            irc.reply(f"PM'ing last 10 results to {msg.nick}")

            # Use link_pool directly here to avoid nested thread_pool deadlock
            link_futures = {
                result[0]: self.link_pool.submit(self.get_all_links, result[0])
                for result in results
            }

            nuke_status = {
                '1': ("\x0304Nuked\x03", "\x0304"),
                '2': ("\x0303UnNuked\x03", "\x0303"),
                '3': ("\x0305ModNuked\x03", "\x0305"),
                '4': ("\x0305DelPred\x03", "\x0305"),
                '5': ("\x0303UnDelPred\x03", "\x0303")
            }

            messages = []
            for result in results:
                releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet = result

                section_formatted = self.section_colors.get(section, section)
                time_ago_str, time_color = self.format_time_ago(unixtime)
                pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")

                time_ago = f"{time_color}{time_ago_str}\x03"
                pretime_colored = f"{time_color}{pretime_formatted}\x03"

                nfo_text, sfv_text, srr_text = link_futures[releasename].result()

                info_string = f"[ \x033INFO\x03: {size} MB, {files} Files ] " if size and files else ""
                section_and_genre = f"[ {section_formatted} / {genre} ]" if genre and genre.lower() != 'null' else f"[ {section_formatted} ]"

                status, color = nuke_status.get(nuked, ("", ""))
                nuked_details = f"[ {status}: {color}{reason or 'No reason'}\x03 => {color}{nukenet or 'Unknown'}\x03 ]" if status else ""

                message = (
                    f"\x033[ PRED ]\x03 [ {releasename} ] "
                    f"[ \x033TIME\x03: {time_ago} / {pretime_colored} ] "
                    f"in {section_and_genre} {info_string}{nuked_details}"
                    f"{nfo_text}{sfv_text}{srr_text}"
                )
                messages.append(message)

            for message in messages:
                irc.reply(message, private=True)

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    dupe = commands.wrap(dupe, ['text'])

    # =====
    # GROUP
    # =====
    def group(self, irc, msg, args, groupname):
        """
        Retrieve comprehensive statistics for a release group.
        
        This command queries the database to provide optimized group statistics including
        release counts, nuke/unnuke information, and historical release data.
        
        Parameters
        ----------
        irc : object
            The IRC connection object
        msg : object
            The message object containing command metadata
        args : list
            Command arguments (unused in this function)
        groupname : str
            The name of the release group to query
        
        Returns
        -------
        None
            Sends formatted replies directly to IRC channel
        
        Displays
        --------
        - Total number of releases
        - Number of nuked releases
        - Number of unnuked releases  
        - First release name and timestamp
        - Last release name and timestamp
        
        Examples
        --------
        +group SCENE
        -> [ GROUP ] [ SCENE ] [ Releases: 150 ] [ NUKES: 5 ] [ UNNUKES: 2 ]
        -> [ FIRST RELEASE ] SCENE-SomeOldRelease [ Time: 2020-01-01 12:00:00 ]
        -> [ LAST RELEASE ] SCENE-SomeNewRelease [ Time: 2023-12-01 15:30:00 ]
        
        Notes
        -----
        - Uses optimized SQL query for better performance
        - Handles groups with no releases gracefully
        - Timestamps are displayed in UTC format
        - Color-coded output for better readability in IRC
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS total_releases,
                        SUM(nuked = '1') AS nukes,
                        SUM(nuked = '2') AS unnukes,
                        SUM(nuked = '3') AS modnukes,
                        SUM(nuked = '4') AS delpres,
                        SUM(nuked = '5') AS undelpres,                                
                        MIN(unixtime) AS first_pre_time,
                        MAX(unixtime) AS last_pre_time,
                        (SELECT releasename FROM releases WHERE grp = %s ORDER BY unixtime ASC LIMIT 1),
                        (SELECT releasename FROM releases WHERE grp = %s ORDER BY unixtime DESC LIMIT 1)
                    FROM releases
                    WHERE grp = %s
                """, (groupname, groupname, groupname))

                result = cursor.fetchone()
                if not result or not result[0]:
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                    return

                total, nukes, unnukes, modnukes, delpres, undelpres, first_time, last_time, first_release, last_release = result
                
                # Use format_time_ago for colored timestamps
                if first_time:
                    first_time_str, first_color = self.format_time_ago(first_time)
                    first_time_fmt = f"{first_color}{first_time_str}\x03"
                else:
                    first_time_fmt = "N/A"
                    
                if last_time:
                    last_time_str, last_color = self.format_time_ago(last_time)
                    last_time_fmt = f"{last_color}{last_time_str}\x03"
                else:
                    last_time_fmt = "N/A"

                irc.reply(f"\x033[ GROUP ]\x03 [ {groupname} ] [ \x033Releases\x03: {total} ] "
                        f"[ \x0305NUKES\03: {nukes} ] [ \x0305MODNUKES\03: {modnukes} ] [ \x033UNNUKES\03: {unnukes} ] [ \x0305DELPRES\03: {delpres} ] [ \x033UNDELPRES\03: {undelpres} ]")
                
                if first_release:
                    irc.reply(f"\x037[ FIRST RELEASE\x03 ] {first_release} pred [ {first_time_fmt} ]")
                if last_release:
                    irc.reply(f"\x033[ LAST RELEASE\x03 ] {last_release} pred [ {last_time_fmt} ]")

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")
    group = commands.wrap(group, ['text'])

# ========
# LASTNUKE
# ========
    @wrap([optional('text')])
    def lastnuke(self, irc, msg, args, groupname=None):
        """
        Retrieve the most recent nuked release from the database.
        
        Fetches the latest nuked release with detailed information including release name,
        timestamp, section, nuke reason, and nuking network. Optionally filters results
        by a specific group name.

        Args:
            irc: IRC connection object for sending replies
            msg: Message object containing command context
            args: Command arguments (unused in this function)
            groupname (str, optional): Specific group name to filter results by

        Returns:
            None: Sends formatted reply directly to IRC channel

        Raises:
            Exception: Logs database errors and returns user-friendly error message

        Examples:
            !lastnuke
                Returns: Latest nuked release across all groups
            
            !lastnuke SCENE
                Returns: Latest nuked release from group "SCENE"

        Reply Format:
            [ NUKED ] [ ReleaseName ] pred [ X time ago / YYYY-MM-DD HH:MM:SS GMT ] 
            in [ Section ] [ Reason => Nuking Network ]
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet
                    FROM releases
                    WHERE nuked = '1'
                """
                params = []

                if groupname:
                    query += " AND grp = %s"
                    params.append(groupname)

                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            releasename, unixtime, section, reason, nukenet = result
            section_formatted = self.section_colors.get(section, section)
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"
            
            irc.reply(
                f"[ \x0305NUKED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_colored} ] "
                f"in [ {section_formatted} ] [ \x0305{reason or 'Unknown reason'}\x03 => \x0305{nukenet or 'Unknown network'}\x03 ]"
            )
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

# ==========
# LASTUNNUKE
# ==========
    @wrap([optional('text')])
    def lastunnuke(self, irc, msg, args, groupname=None):
        """
        Retrieve the most recent unnuked release from the database.
        
        Fetches release information for the latest unnuked entry, optionally filtered
        by a specific release group. Returns formatted release details including
        release name, pre-time, section, unnuke reason, and nuke network.
        
        Args:
            irc: IRC connection object for sending replies
            msg: Message object containing command metadata
            args: Command arguments (unused in this function)
            groupname (str, optional): Specific group name to filter results by.
                                    If provided, only returns unnukes from this group.
        
        Returns:
            None: Sends formatted response directly via IRC reply
            
        Raises:
            Exception: Logs database errors and returns error message to user
            
        Examples:
            !lastunnuke
                → Returns the most recent unnuked release across all groups
                
            !lastunnuke SCENE
                → Returns the most recent unnuked release from group "SCENE"
                
        Response Format:
            [ UNNUKED ] [ ReleaseName ] pred [ X hours ago / YYYY-MM-DD HH:MM:SS GMT ] 
            in [ Section ] [ Reason => Network ]
            
        Notes:
            - Unnuked releases are identified by nuked = 2 in the database
            - Time displays are colored based on how recent the unnuke was
            - Sections are color-coded according to section_colors mapping
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Base query to fetch the most recent unnuked release
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet
                    FROM releases
                    WHERE nuked = '2'
                """
                params = []

                # Add group filter if provided
                if groupname:
                    query += " AND grp = %s"
                    params.append(groupname)

                # Get the most recent result
                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            # Process result
            releasename, unixtime, section, reason, nukenet = result
            section_formatted = self.section_colors.get(section, section)
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"
            
            irc.reply(
                f"[ \x0303UNNUKED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_colored} ] "
                f"in [ {section_formatted} ] [ \x0303{reason or 'Unknown reason'}\x03 => \x0303{nukenet or 'Unknown network'}\x03 ]"
            )
            
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

# ===========
# LASTMODNUKE
# ===========
    @wrap([optional('text')])
    def lastmodnuke(self, irc, msg, args, groupname=None):
        """
        Retrieve the most recent modnuked release from the database.
        
        A modnuke is identified by the nuked status value of 3. This command
        fetches the latest modnuke entry and returns formatted information
        including release name, timestamp, section, reason, and nukenet.
        
        Parameters
        ----------
        irc : object
            The IRC connection object
        msg : object
            The message object containing command metadata
        args : list
            Command arguments (unused in this function)
        groupname : str, optional
            Filter results by specific group name. If provided, only returns
            modnukes from this group.
        
        Returns
        -------
        None
            Sends formatted response directly via IRC reply
        
        Examples
        --------
        >>> lastmodnuke
        [ MODNUKED ] [ Some.Release.Group.S01E01 ] pred [ 2 hours ago / 2023-10-15 14:30:00 GMT ] 
        in [ TV ] [ Duplicate => SomeNukeNet ]
        
        >>> lastmodnuke SomeGroup
        [ MODNUKED ] [ SomeGroup.Release.S01E01 ] pred [ 1 day ago / 2023-10-14 14:30:00 GMT ] 
        in [ TV ] [ Bad quality => SomeNukeNet ]
        
        Notes
        -----
        - Uses nuked = 3 to identify modnuked releases
        - Results are ordered by timestamp (newest first)
        - Includes colored formatting for better IRC visibility
        - Handles cases where no modnukes are found with appropriate messaging
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Base query to fetch the most recent unnuked release
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet
                    FROM releases
                    WHERE nuked = '3'
                """
                params = []

                # Add group filter if provided
                if groupname:
                    query += " AND grp = %s"
                    params.append(groupname)

                # Get the most recent result
                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            # Process result
            releasename, unixtime, section, reason, nukenet = result
            section_formatted = self.section_colors.get(section, section)
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"
            
            irc.reply(
                f"[ \x0305MODNUKED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_colored} ] "
                f"in [ {section_formatted} ] [ \x0305{reason or 'Unknown reason'}\x03 => \x0305{nukenet or 'Unknown network'}\x03 ]"
            )
            
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")           

# ===========
# LASTDELPRE
# ===========
    @wrap([optional('text')])
    def lastdelpre(self, irc, msg, args, groupname=None):
        """
        Retrieve the most recent delpre release from the database.
        
        A delpre is identified by the nuked status value of 4. This command
        fetches the latest delpre entry and returns formatted information
        including release name, timestamp, section, reason, and nukenet.
        
        Parameters
        ----------
        irc : object
            The IRC connection object
        msg : object
            The message object containing command metadata
        args : list
            Command arguments (unused in this function)
        groupname : str, optional
            Filter results by specific group name. If provided, only returns
            modnukes from this group.
        
        Returns
        -------
        None
            Sends formatted response directly via IRC reply
        
        Examples
        --------
        >>> lastdelpre
        [ DELPRE ] [ Some.Release.Group.S01E01 ] pred [ 2 hours ago / 2023-10-15 14:30:00 GMT ] 
        in [ TV ] [ Duplicate => SomeNukeNet ]
        
        >>> lastmodnuke SomeGroup
        [ DELPRE ] [ SomeGroup.Release.S01E01 ] pred [ 1 day ago / 2023-10-14 14:30:00 GMT ] 
        in [ TV ] [ Bad quality => SomeNukeNet ]
        
        Notes
        -----
        - Uses delpre = 4 to identify delpre releases
        - Results are ordered by timestamp (newest first)
        - Includes colored formatting for better IRC visibility
        - Handles cases where no modnukes are found with appropriate messaging
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Base query to fetch the most recent unnuked release
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet
                    FROM releases
                    WHERE nuked = '4'
                """
                params = []

                # Add group filter if provided
                if groupname:
                    query += " AND grp = %s"
                    params.append(groupname)

                # Get the most recent result
                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            # Process result
            releasename, unixtime, section, reason, nukenet = result
            section_formatted = self.section_colors.get(section, section)
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"
            
            irc.reply(
                f"[ \x0305DELPRED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_colored} ] "
                f"in [ {section_formatted} ] [ \x0305{reason or 'Unknown reason'}\x03 => \x0305{nukenet or 'Unknown'}\x03 ]"
            )
            
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

# ============
# LASTUNDELPRE
# ============
    @wrap([optional('text')])
    def lastundelpre(self, irc, msg, args, groupname=None):
        """
        Retrieve the most recent undelpre release from the database.
        
        A undelpre is identified by the nuked status value of 4. This command
        fetches the latest undelpre entry and returns formatted information
        including release name, timestamp, section, reason, and nukenet.
        
        Parameters
        ----------
        irc : object
            The IRC connection object
        msg : object
            The message object containing command metadata
        args : list
            Command arguments (unused in this function)
        groupname : str, optional
            Filter results by specific group name. If provided, only returns
            modnukes from this group.
        
        Returns
        -------
        None
            Sends formatted response directly via IRC reply
        
        Examples
        --------
        >>> lastundelpre
        [ UNDELPRE ] [ Some.Release.Group.S01E01 ] pred [ 2 hours ago / 2023-10-15 14:30:00 GMT ] 
        in [ TV ] [ Duplicate => SomeNukeNet ]
        
        >>> lastmodnuke SomeGroup
        [ UNDELPRE ] [ SomeGroup.Release.S01E01 ] pred [ 1 day ago / 2023-10-14 14:30:00 GMT ] 
        in [ TV ] [ Bad quality => SomeNukeNet ]
        
        Notes
        -----
        - Uses nuked = 5 to identify undelpre releases
        - Results are ordered by timestamp (newest first)
        - Includes colored formatting for better IRC visibility
        - Handles cases where no modnukes are found with appropriate messaging
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Base query to fetch the most recent unnuked release
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet
                    FROM releases
                    WHERE nuked = '5'
                """
                params = []

                # Add group filter if provided
                if groupname:
                    query += " AND grp = %s"
                    params.append(groupname)

                # Get the most recent result
                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                return

            # Process result
            releasename, unixtime, section, reason, nukenet = result
            section_formatted = self.section_colors.get(section, section)
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"
            
            irc.reply(
                f"[ \x0303UNDELPRED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_colored} ] "
                f"in [ {section_formatted} ] [ \x0305{reason or 'Unknown reason'}\x03 => \x0305{nukenet or 'Unknown'}\x03 ]"
            )
            
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # =======
    # SECTION
    # =======          
    def section(self, irc, msg, args, section=None):
        """
        Retrieve the 10 most recent releases, optionally filtered by section.
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Base query to fetch ALL releases filtered by section
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet,
                        size,
                        files,
                        nuked
                    FROM releases
                    WHERE 1=1
                """
                params = []

                # If a section is provided, add it to the query
                if section:
                    query += " AND section = %s"
                    params.append(section)

                # Order by most recent releases, limit to 10
                query += " ORDER BY unixtime DESC LIMIT 10"

                # Execute the query
                cursor.execute(query, params)
                results = cursor.fetchall()

            if not results:
                irc.reply(f"[ \x0305No releases found in section: {section or 'all sections'}, that makes me a sad pre bot :-(\x03 ]")
                return
            
            # Notify the user about sending results
            section_display = section or "all sections"
            irc.reply(f"PM'ing last 10 results from {section_display} to {msg.nick}")

            # Process all results efficiently
            messages = []
            for result in results:
                releasename, unixtime, section, reason, nukenet, size, files, nuked = result
                section_formatted = self.section_colors.get(section, section)
                
                # Get time_ago string and color
                time_ago_str, time_color = self.format_time_ago(unixtime)
                pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
                
                # Apply the same color to both time displays
                time_ago = f"{time_color}{time_ago_str}\x03"
                pretime_colored = f"{time_color}{pretime_formatted}\x03"
                
                # Build info string if data exists
                info_string = f"[ INFO: {size} MB, {files} Files ] " if size and files else ""
                
                # Determine nuke status and format accordingly - COMPARE AGAINST STRINGS
                if nuked == '1':  # Changed to string '1'
                    nuked_details = f"[ \x0304Nuked\x03: \x0304{reason or 'No reason'}\x03 => \x0304{nukenet or 'Unknown'}\x03 ]"
                elif nuked == '2':  # Changed to string '2'
                    nuked_details = f"[ \x0303UnNuked\x03: \x0303{reason or 'No reason'}\x03 => \x0303{nukenet or 'Unknown'}\x03 ]"
                elif nuked == '3':  # Changed to string '3'
                    nuked_details = f"[ \x0305ModNuked\x03: \x0305{reason or 'No reason'}\x03 => \x0305{nukenet or 'Unknown'}\x03 ]"
                elif nuked == '4': 
                    nuked_details = f"[ \x0305DelPred\x03: \x0305{reason or 'No reason'}\x03 => \x0305{nukenet or 'Unknown'}\x03 ]"
                elif nuked == '5': 
                    nuked_details = f"[ \x0304UnDelPred\x03: \x0305{reason or 'No reason'}\x03 => \x0305{nukenet or 'Unknown'}\x03 ]"
                else:
                    nuked_details = ""

                message = (
                    f"[ \x033PRED\x03 ] [ {releasename} ] "
                    f"pred [ {time_ago} / {pretime_colored} ] "
                    f"in [ {section_formatted} ] "
                    f"{info_string}"
                    f"{nuked_details}"
                )
                messages.append(message)

            # Send all messages to the user
            for message in messages:
                irc.reply(message, private=True)

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")
    section = commands.wrap(section, ['text'])

    def _check_and_process_pending_url_sync(self, conn, releasename):
        """Check and process pending URL synchronously within the same database transaction"""
        try:
            cursor = conn.cursor()
            
            # Check if there's a pending URL for this release
            cursor.execute(
                "SELECT url FROM pending_urls WHERE releasename = %s",
                (releasename,)
            )
            result = cursor.fetchone()
            
            if result:
                url = result[0]
                
                # Update the release with the pending URL
                cursor.execute(
                    "UPDATE releases SET url = %s WHERE releasename = %s",
                    (url, releasename),
                )
                
                # Remove from pending_urls
                cursor.execute(
                    "DELETE FROM pending_urls WHERE releasename = %s",
                    (releasename,)
                )
                
                # Update cache
                with self.pending_urls_lock:
                    if releasename in self.pending_urls_cache:
                        del self.pending_urls_cache[releasename]
                
                self.log.info(f"✓ Processed pending URL for {releasename} during addpre")
                
                # Track success
                self.url_stats['delayed_success'] += 1
                
        except Exception as e:
            self.log.error(f"Error processing pending URL sync for {releasename}: {e}")

    def _load_pending_urls_from_db(self):
        """Load pending URLs from database on startup"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Check if the table exists first
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = 'pending_urls'
                """, (self.db_config['database'],))
                
                table_exists = cursor.fetchone()[0] > 0
                
                if not table_exists:
                    self.log.warning("pending_urls table does not exist, skipping load")
                    return
                    
                cursor.execute("SELECT * FROM pending_urls")
                results = cursor.fetchall()
                
                for row in results:
                    releasename = row[1]
                    entry = {
                        'releasename': releasename,
                        'url': row[2],
                        'attempt_count': row[3],
                        'max_attempts': row[4],
                        'retry_delay': row[5],
                        'timestamp': row[6]
                    }
                    
                    with self.pending_urls_lock:
                        self.pending_urls_cache[releasename] = entry
                        self.pending_urls.put(entry.copy())
                        
                self.log.info(f"Loaded {len(results)} pending URLs from database")
                
        except Exception as e:
            self.log.error(f"Error loading pending URLs from DB: {e}")

    def _add_to_pending_queue_db(self, entry):
        """Add pending URL to database"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO pending_urls 
                    (releasename, url, attempt_count, max_attempts, retry_delay, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    url = VALUES(url),
                    attempt_count = VALUES(attempt_count),
                    timestamp = VALUES(timestamp)
                """, (
                    entry['releasename'],
                    entry['url'],
                    entry['attempt_count'],
                    entry['max_attempts'],
                    entry['retry_delay'],
                    entry['timestamp']
                ))
                conn.commit()
                
                with self.pending_urls_lock:
                    self.pending_urls_cache[entry['releasename']] = entry
                    self.pending_urls.put(entry.copy())
                    
                return True
        except Exception as e:
            self.log.error(f"Error adding to pending queue DB: {e}")
            return False

    def _remove_from_pending_db(self, releasename):
        """Remove pending URL from database"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM pending_urls WHERE releasename = %s", (releasename,))
                conn.commit()
                
                with self.pending_urls_lock:
                    if releasename in self.pending_urls_cache:
                        del self.pending_urls_cache[releasename]
                        
                return True
        except Exception as e:
            self.log.error(f"Error removing from pending DB: {e}")
            return False

    def _update_pending_attempt_count(self, releasename, attempt_count):
        """Update attempt count in database"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE pending_urls 
                    SET attempt_count = %s, timestamp = %s
                    WHERE releasename = %s
                """, (attempt_count, time.time(), releasename))
                conn.commit()
                return True
        except Exception as e:
            self.log.error(f"Error updating attempt count for {releasename}: {e}")
            return False
    
    # ============
    # PENDING URLS
    # ============
    def _process_pending_urls(self):
        """Background thread to process pending URLs - IMPROVED VERSION"""
        while True:
            try:
                # Wait for items in the queue
                pending_entry = self.pending_urls.get(timeout=5)
                
                releasename = pending_entry['releasename']
                url = pending_entry['url']
                attempt_count = pending_entry['attempt_count']
                max_attempts = pending_entry['max_attempts']
                retry_delay = pending_entry['retry_delay']
                timestamp = pending_entry.get('timestamp', time.time())  # Fallback to current time if missing
                
                # Check if entry has expired (TTL: 24 hours = 86400 seconds)
                age = time.time() - timestamp
                if age > 86400:  # 24 hours
                    self.log.warning(f"TTL expired for {releasename} (age: {age:.0f}s), removing from queue")
                    self.url_stats['failed'] += 1
                    self._remove_from_pending_db(releasename)
                    continue
                
                # Check if we should retry
                if attempt_count >= max_attempts:
                    self.log.warning(f"Max retries reached for {releasename}, removing from queue")
                    self.url_stats['failed'] += 1
                    self._remove_from_pending_db(releasename)
                    continue
                
                try:
                    with self.db_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT releasename FROM releases WHERE releasename = %s",
                            (releasename,)
                        )
                        existing = cursor.fetchone()
                        
                        if existing:
                            # Release now exists! Update the URL
                            cursor.execute(
                                "UPDATE releases SET url = %s WHERE releasename = %s",
                                (url, releasename),
                            )
                            conn.commit()
                            
                            self.log.info(f"✓ Processed pending URL for {releasename} (attempt {attempt_count + 1})")
                            
                            # Clean up from database
                            self._remove_from_pending_db(releasename)
                            
                            # Track delayed success
                            self.url_stats['delayed_success'] += 1
                        else:
                            # Release still doesn't exist, schedule retry
                            new_attempt_count = attempt_count + 1
                            
                            # Update attempt count in database
                            if self._update_pending_attempt_count(releasename, new_attempt_count):
                                # Calculate next retry time
                                delay = retry_delay * new_attempt_count
                                next_retry = time.time() + delay
                                pending_entry['attempt_count'] = new_attempt_count
                                pending_entry['next_retry'] = next_retry
                                
                                # Update local cache
                                with self.pending_urls_lock:
                                    if releasename in self.pending_urls_cache:
                                        self.pending_urls_cache[releasename] = pending_entry
                                
                                # Schedule delayed requeue using a timer (non-blocking!)
                                timer = threading.Timer(delay, self._requeue_pending_url, args=[pending_entry])
                                timer.daemon = True
                                timer.start()
                                
                                self.log.info(f"⏱ Release {releasename} not found, retry scheduled in {delay}s (attempt {new_attempt_count}/{max_attempts})")
                            
                except Exception as e:
                    self.log.error(f"Error processing pending URL for {releasename}: {e}")
                    
            except queue.Empty:
                # No items in queue, continue waiting
                continue
            except Exception as e:
                self.log.error(f"Error in pending URL processor: {e}")

    def _requeue_pending_url(self, pending_entry):
        """Requeue a pending URL after delay (called by timer)"""
        self.pending_urls.put(pending_entry)
        self.log.debug(f"Requeued {pending_entry['releasename']} for retry") 

    def _check_pending_urls_after_addpre(self, releasename):
        """Check if there are pending URLs for a newly added release - IMPROVED VERSION"""
        with self.pending_urls_lock:
            if releasename in self.pending_urls_cache:
                pending_entry = self.pending_urls_cache[releasename].copy()
                
                # IMMEDIATE PROCESSING: Process right now instead of waiting in queue
                self.log.info(f"⚡ Immediate processing triggered for pending URL: {releasename}")
                
                # Submit to thread pool for immediate processing
                self.thread_pool.submit(self._process_pending_url_immediately, pending_entry)

    def _process_pending_url_immediately(self, pending_entry):
        """Process a pending URL immediately when its release is added"""
        releasename = pending_entry['releasename']
        url = pending_entry['url']
        
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Double-check the release exists
                cursor.execute(
                    "SELECT releasename FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Update the URL
                    cursor.execute(
                        "UPDATE releases SET url = %s WHERE releasename = %s",
                        (url, releasename),
                    )
                    conn.commit()
                    
                    self.log.info(f"✓ Immediately processed pending URL for {releasename}")
                    
                    # Clean up from database and cache
                    self._remove_from_pending_db(releasename)
                    
                    # Track success
                    self.url_stats['delayed_success'] += 1
                else:
                    # Race condition - release doesn't exist yet, requeue normally
                    self.pending_urls.put(pending_entry)
                    self.log.warning(f"Release {releasename} not found during immediate processing, requeued")
                    
        except Exception as e:
            self.log.error(f"Error immediately processing pending URL for {releasename}: {e}")
            # Requeue on error
            self.pending_urls.put(pending_entry)                

    # ===========
    # PENDINGURLS
    # ===========
    def pendingurls(self, irc, msg, args):
        """Show pending URLs waiting for releases to be added"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM pending_urls ORDER BY timestamp DESC LIMIT 10")
                results = cursor.fetchall()
                
                if not results:
                    irc.reply("[ No pending URLs waiting for releases ]")
                    return
                
                irc.reply(f"PM'ing {len(results)} pending URLs to {msg.nick}")
                
                for row in results:
                    releasename = row[1]
                    url = row[2]
                    attempt_count = row[3]
                    max_attempts = row[4]
                    timestamp = row[6]
                    
                    # Calculate time ago
                    time_ago_str, _ = self.format_time_ago(timestamp)
                    
                    message = f"[ {releasename} ] → {url} (attempts: {attempt_count}/{max_attempts}, queued: {time_ago_str})"
                    irc.reply(message, private=True)
        
        except Exception as e:
            self.log.error(f"Error in pendingurls: {e}")
            irc.reply(f"Error retrieving pending URLs: {str(e)}")

    @admin_only
    def processpendingurls(self, irc, msg, args):
            """Manually process all pending URLs and match with existing releases"""
                      
            try:
                with self.db_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Get all pending URLs
                    cursor.execute("SELECT releasename, url FROM pending_urls")
                    pending_entries = cursor.fetchall()
                    
                    if not pending_entries:
                        irc.reply("No pending URLs to process")
                        return
                    
                    processed_count = 0
                    not_found_count = 0
                    error_count = 0
                    
                    irc.reply(f"Processing {len(pending_entries)} pending URLs...")
                    
                    for releasename, url in pending_entries:
                        try:
                            # Check if release exists
                            cursor.execute(
                                "SELECT releasename FROM releases WHERE releasename = %s",
                                (releasename,)
                            )
                            existing = cursor.fetchone()
                            
                            if existing:
                                # Release exists! Update the URL
                                cursor.execute(
                                    "UPDATE releases SET url = %s WHERE releasename = %s",
                                    (url, releasename),
                                )
                                
                                # Remove from pending_urls
                                cursor.execute(
                                    "DELETE FROM pending_urls WHERE releasename = %s",
                                    (releasename,)
                                )
                                
                                conn.commit()
                                
                                # Update cache
                                with self.pending_urls_lock:
                                    if releasename in self.pending_urls_cache:
                                        del self.pending_urls_cache[releasename]
                                
                                processed_count += 1
                                self.log.info(f"Processed pending URL for {releasename}")
                                
                                # Track success
                                self.url_stats['delayed_success'] += 1
                            else:
                                not_found_count += 1
                                
                        except Exception as e:
                            error_count += 1
                            self.log.error(f"Error processing {releasename}: {e}")
                    
                    # Final report
                    message = (
                        f"[ Pending URLs Processed ] "
                        f"[ Matched & Updated: \x0303{processed_count}\x03 ] "
                        f"[ Not Found: \x0307{not_found_count}\x03 ] "
                        f"[ Errors: \x0304{error_count}\x03 ]"
                    )
                    irc.reply(message)
                    
            except mysql.connector.Error as e:
                self.log.error(f"Database error {e.errno}: {e.msg}")
                irc.reply("Database error occurred")
            except ValueError as e:
                irc.reply(f"Invalid input: {e}")
        
    processpendingurls = commands.wrap(processpendingurls, [])


    # Optional: Version with detailed output
    @admin_only
    def processpendingurlsverbose(self, irc, msg, args):
        """Manually process all pending URLs with detailed output"""
        
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Get all pending URLs
                cursor.execute("SELECT releasename, url, attempt_count FROM pending_urls")
                pending_entries = cursor.fetchall()
                
                if not pending_entries:
                    irc.reply("No pending URLs to process")
                    return
                
                irc.reply(f"Processing {len(pending_entries)} pending URLs, sending results to PM...")
                
                processed_count = 0
                not_found_count = 0
                error_count = 0
                
                for releasename, url, attempt_count in pending_entries:
                    try:
                        # Check if release exists
                        cursor.execute(
                            "SELECT releasename FROM releases WHERE releasename = %s",
                            (releasename,)
                        )
                        existing = cursor.fetchone()
                        
                        if existing:
                            # Release exists! Update the URL
                            cursor.execute(
                                "UPDATE releases SET url = %s WHERE releasename = %s",
                                (url, releasename),
                            )
                            
                            # Remove from pending_urls
                            cursor.execute(
                                "DELETE FROM pending_urls WHERE releasename = %s",
                                (releasename,)
                            )
                            
                            conn.commit()
                            
                            # Update cache
                            with self.pending_urls_lock:
                                if releasename in self.pending_urls_cache:
                                    del self.pending_urls_cache[releasename]
                            
                            processed_count += 1
                            self.url_stats['delayed_success'] += 1
                            
                            # Send detailed success message
                            irc.reply(
                                f"[\x0303✓\x03] {releasename} → URL added (was pending {attempt_count} attempts)",
                                private=True
                            )
                        else:
                            not_found_count += 1
                            irc.reply(
                                f"[\x0307○\x03] {releasename} → Release not found yet",
                                private=True
                            )
                            
                    except Exception as e:
                        error_count += 1
                        self.log.error(f"Error processing {releasename}: {e}")
                        irc.reply(
                            f"[\x0304✗\x03] {releasename} → Error: {str(e)}",
                            private=True
                        )
                
                # Final summary
                message = (
                    f"[ Summary ] "
                    f"[ Matched: \x0303{processed_count}\x03 ] "
                    f"[ Not Found: \x0307{not_found_count}\x03 ] "
                    f"[ Errors: \x0304{error_count}\x03 ]"
                )
                irc.reply(message)
                
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")
    
    processpendingurlsverbose = commands.wrap(processpendingurlsverbose, [])


    # Optional: Process a single pending URL by releasename
    @admin_only
    def processpendingurl(self, irc, msg, args, releasename):
        """<releasename> - Process a specific pending URL"""

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Get the pending URL
                cursor.execute(
                    "SELECT url FROM pending_urls WHERE releasename = %s",
                    (releasename,)
                )
                result = cursor.fetchone()
                
                if not result:
                    irc.reply(f"No pending URL found for: {releasename}")
                    return
                
                url = result[0]
                
                # Check if release exists
                cursor.execute(
                    "SELECT releasename FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Release exists! Update the URL
                    cursor.execute(
                        "UPDATE releases SET url = %s WHERE releasename = %s",
                        (url, releasename),
                    )
                    
                    # Remove from pending_urls
                    cursor.execute(
                        "DELETE FROM pending_urls WHERE releasename = %s",
                        (releasename,)
                    )
                    
                    conn.commit()
                    
                    # Update cache
                    with self.pending_urls_lock:
                        if releasename in self.pending_urls_cache:
                            del self.pending_urls_cache[releasename]
                    
                    self.url_stats['delayed_success'] += 1
                    
                    irc.reply(f"✓ Processed pending URL for: {releasename}")
                else:
                    irc.reply(f"Release not found in database: {releasename}")
                    
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")
    
    processpendingurl = commands.wrap(processpendingurl, ['text'])            

    # =========
    # URL STATS
    # =========    
    def urlstats(self, irc, msg, args):
        """Show URL processing statistics"""
        stats = self.url_stats
        total_processed = stats['immediate_success'] + stats['delayed_success']
        success_rate = (total_processed / max(1, stats['queued'] + stats['immediate_success'])) * 100
        
        message = (
            f"[ URL Processing Stats ] "
            f"[ Immediate: {stats['immediate_success']} ] "
            f"[ Queued: {stats['queued']} ] "
            f"[ Delayed Success: {stats['delayed_success']}] "
            f"[ Failed: {stats['failed']} ] "
            f"[ Success Rate: {success_rate:.1f}% ]"
        )
        irc.reply(message)

    urlstats = commands.wrap(urlstats, [])      

    # ========================
    # BACKGROUND TASK HANDLING
    # ========================
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addpre(self, irc, msg, args):
        """Threadpool-based addpre handler"""
            
        if len(args) < 2:
            irc.reply("Usage: !addpre <releasename> <section>")
            return
        
        releasename, section = args[0], args[1]
        group = releasename.split('-')[-1] if '-' in releasename else None
        
        # Submit to thread pool
        self.thread_pool.submit(self._addpre_thread, irc, releasename, section, group)

    def _addpre_thread(self, irc, releasename, section, group):
        """Fast addpre with background URL fetching"""
        try:
            self.log.debug(f"Starting _addpre_thread for: {releasename}")
            
            with self.db_connection() as conn:
                self.log.debug("Database connection successful")
                
                cursor = conn.cursor()
                
                # FAST INSERT - no URL yet (url column will be NULL)
                cursor.execute(
                    "INSERT IGNORE INTO releases (releasename, section, grp) VALUES (%s, %s, %s)",
                    (releasename, section, group),
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    self.log.debug("New release, announcing...")
                    
                    # Process any pending URLs from old !addurl queue
                    self._check_and_process_pending_url_sync(conn, releasename)
                    
                    # Announce immediately (fast - no API delay)
                    self.announce_pre(irc, releasename, section)
                    
                    # Fetch URL in background (non-blocking)
                    self.thread_pool.submit(self._background_url_fetch, releasename)
                    self.log.debug(f"Background URL fetch queued for {releasename}")
                    
                    # AUTO-DISCOGS: Lookup Discogs URL for music releases (using thread pool)
                    if self.registryValue('autoDiscogs') and section in self.MUSIC_SECTIONS:
                        # PASS IRC OBJECT for announcements
                        self.thread_pool.submit(self._background_discogs_lookup, releasename, irc)
                        self.log.debug(f"Auto-Discogs lookup queued for {releasename}")
                    
                else:
                    self.log.debug("Release already exists")
                    
        except Exception as e:
            self.log.error(f"Addpre error: {repr(e)}")

    # ========
    # ADDGENRE
    # ========
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addgenre(self, irc, msg, args):
        """Threadpool-based genre handler"""
        
        if len(args) < 2:
            irc.reply("Usage: !gn <releasename> <genre>")
            return
        
        releasename = args[0]
        # Replace forward slashes with underscores in genre
        genre = args[1].replace("/", "_")
        
        # Log what we're processing
        self.log.info(f"Processing !gn command: release='{releasename}', genre='{genre}'")
        
        # Submit to thread pool
        self.thread_pool.submit(self._addgenre_thread, irc, releasename, genre)

    def _addgenre_thread(self, irc, releasename, genre):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET genre = %s WHERE releasename = %s",
                    (genre, releasename),
                )
                conn.commit()
                
            # Announce to channel after successful DB update
            self.announce_genre(irc, releasename, genre)
                
        except Exception as e:
            self.log.error(f"Addgenre error: {e}")      

    # =======
    # ADDURL
    # =======
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addurl(self, irc, msg, args):
        """Threadpool-based URL handler"""
        
        if len(args) < 2:
            irc.reply("Usage: !addurl <releasename> <url>")
            return
        
        releasename = args[0]
        url = args[1]
        
        # Log what we're processing
        #self.log.info(f"Processing !addurl command: release='{releasename}', url='{url}'")
        
        # Submit to thread pool
        self.thread_pool.submit(self._addurl_thread, irc, releasename, url)

    def _addurl_thread(self, irc, releasename, url):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # First check if the release exists
                cursor.execute(
                    "SELECT releasename FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                existing = cursor.fetchone()
                
                if not existing:                    
                    self.log.warning(f"No release found with name: {releasename} - checking if already queued")
                    
                    # Check if this release is already in the pending queue
                    cursor.execute(
                        "SELECT releasename FROM pending_urls WHERE releasename = %s",
                        (releasename,)
                    )
                    already_queued = cursor.fetchone()
                    
                    if already_queued:
                        # Already queued - just update the URL if it's different
                        cursor.execute(
                            "UPDATE pending_urls SET url = %s, attempt_count = 0, timestamp = %s WHERE releasename = %s",
                            (url, time.time(), releasename)
                        )
                        conn.commit()
                        self.log.info(f"Release {releasename} already queued - updated URL")
                        return
                    
                    # Not queued yet - add to queue
                    self.url_stats['queued'] += 1
                    pending_entry = {
                        'releasename': releasename,
                        'url': url,
                        'attempt_count': 0,
                        'timestamp': time.time(),
                        'max_attempts': 10,
                        'retry_delay': 30
                    }
                    
                    # Add to database-persisted queue
                    if self._add_to_pending_queue_db(pending_entry):
                        self.log.info(f"Queued URL for later processing: {releasename}")
                    else:
                        self.log.error(f"Error queuing URL for {releasename}")
                    return
                
                ### ADD THIS BLOCK START ###
                # Check if this is a music release - skip URL addition for music
                cursor.execute(
                    "SELECT section FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                section_result = cursor.fetchone()
                if section_result:
                    section = section_result[0]
                    music_sections = {
                        'MP3', 'MP3-WEB', 'FLAC', 'FLAC-WEB', 'FLACFR', 
                        'FLAC-FR', 'ABOOK', 'MVID', 'MViD', 'MDVDR', 'MBLURAY'
                    }
                    if section in music_sections:
                        self.log.info(f"Skipping URL addition for music release: {releasename} (section: {section})")
                        return
                ### ADD THIS BLOCK END ###
                
                # Release exists and is not music - update it directly
                cursor.execute(
                    "UPDATE releases SET url = %s WHERE releasename = %s",
                    (url, releasename),
                )
                conn.commit()
                
                # Track immediate success
                self.url_stats['immediate_success'] += 1                    
                
                # Clean up from pending cache/db if it was there
                self._remove_from_pending_db(releasename)
                
                # Announce to channel after successful DB update
                self.announce_url(irc, releasename, url)
                        
        except Exception as e:
            self.log.error(f"Addurl error: {e}")

    # =======
    # ADDNUKE
    # =======
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addnuke(self, irc, msg, args):
        """Threadpool-based nuke handler"""
        try:
                
            if len(args) < 3:
                irc.reply("Usage: !nuke <releasename> <reason> <nukenet>")
                return

            releasename = args[0]
            reason = ' '.join(args[1:-1])
            nukenet = args[-1]
            
            self.log.debug(f"Parsed - releasename: {releasename}, reason: {reason}, nukenet: {nukenet}")
            
            # Submit to thread pool
            self.thread_pool.submit(self._nuke_thread, irc, releasename, reason, nukenet)
            
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # ==========================================================
    # Add nukes that is not in database to an new_nukes database
    # ==========================================================
    def _nuke_thread(self, irc, releasename, reason, nukenet):
        """Nuke handler"""
        self._dispatch_nuke_thread(irc, 'nuke', releasename, reason, nukenet)

    # =======
    # DELPRE
    # =======
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_adddelpre(self, irc, msg, args):
        """Threadpool-based delpre handler"""
        try:

            if len(args) < 3:
                irc.reply("Usage: !delpre <releasename> <reason> <nukenet>")
                return

            releasename = args[0]
            reason = ' '.join(args[1:-1])
            nukenet = args[-1]
            
            self.log.debug(f"Parsed - releasename: {releasename}, reason: {reason}, nukenet: {nukenet}")
            
            # Submit to thread pool
            self.thread_pool.submit(self._delpre_thread, irc, releasename, reason, nukenet)
            
        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # ==========================================================
    # Add nukes that is not in database to an new_nukes database
    # ==========================================================
    def _delpre_thread(self, irc, releasename, reason, nukenet):
        """Delpre handler"""
        self._dispatch_nuke_thread(irc, 'delpre', releasename, reason, nukenet)

    # =========
    # UNDELPRE
    # =========
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addundelpre(self, irc, msg, args):
        """Threadpool-based undelpre handler"""
            
        if len(args) < 3:
            irc.reply("Usage: !undelpre <releasename> <reason> <nukenet>")
            return

        releasename = args[0]
        reason = ' '.join(args[1:-1])
        nukenet = args[-1]
        
        # Submit to thread pool
        self.thread_pool.submit(self._undelpre_thread, irc, releasename, reason, nukenet)

    # ==========================================================
    # Add unnukes that is not in database to an new_nukes database
    # ==========================================================
    def _undelpre_thread(self, irc, releasename, reason, nukenet):
        """Undelpre handler"""
        self._dispatch_nuke_thread(irc, 'undelpre', releasename, reason, nukenet)                            

    # =========
    # ADDUNNUKE
    # =========
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addunnuke(self, irc, msg, args):
        """Threadpool-based unnuke handler"""
            
        if len(args) < 3:
            irc.reply("Usage: !unnuke <releasename> <reason> <nukenet>")
            return

        releasename = args[0]
        reason = ' '.join(args[1:-1])
        nukenet = args[-1]
        
        # Submit to thread pool
        self.thread_pool.submit(self._unnuke_thread, irc, releasename, reason, nukenet)

    # ==========================================================
    # Add unnukes that is not in database to an new_nukes database
    # ==========================================================
    def _unnuke_thread(self, irc, releasename, reason, nukenet):
        """Unnuke handler"""
        self._dispatch_nuke_thread(irc, 'unnuke', releasename, reason, nukenet)

    # =======
    # MODNUKE
    # =======
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addmodnuke(self, irc, msg, args):
        """Threadpool-based modnuke handler"""
            
        if len(args) < 3:
            irc.reply("Usage: !modnuke <releasename> <reason> <nukenet>")
            return

        releasename = args[0]
        reason = ' '.join(args[1:-1])
        nukenet = args[-1]
        
        # Submit to thread pool
        self.thread_pool.submit(self._modnuke_thread, irc, releasename, reason, nukenet)

    # ==========================================================
    # Add modnukes that is not in database to an new_nukes database
    # ==========================================================
    def _modnuke_thread(self, irc, releasename, reason, nukenet):
        """Modnuke handler"""
        self._dispatch_nuke_thread(irc, 'modnuke', releasename, reason, nukenet)

    # =======
    # ADDINFO
    # =======
    @authorized_only(["CTW_PRE", "klapvogn"])
    def handle_addinfo(self, irc, msg, args):
        """Threadpool-based info handler"""
            
        if len(args) < 3:
            irc.reply("Usage: !info <releasename> <files> <size>")
            return

        releasename, files, size = args[0], args[1], args[2]
        
        # Submit to thread pool
        self.thread_pool.submit(self._addinfo_thread, irc, releasename, files, size)

    def _addinfo_thread(self, irc, releasename, files, size):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET files = %s, size = %s WHERE releasename = %s",
                    (files, size, releasename),
                )
                conn.commit()
                
            # Announce to channel after successful DB update
            self.announce_info(irc, releasename, files, size)
                
        except Exception as e:
            self.log.error(f"Addinfo error: {e}")

    # =======================
    # ANNOUNCE PRE TO CHANNEL
    # =======================
    def announce_pre(self, irc, releasename, section):
        """Announce new release to the #omgwtfnzbs.pre channel on another network."""
        #self.log.info(f"Announcing release: {section} {releasename}")
        # Define the target channel
        #target_channel = "#bot"
        #target_channel = "#omgwtfnzbs.pre"
        target_channel = self.registryValue('PreannounceChannel')

        # Lookup the section color
        section_formatted = self.section_colors.get(section, section)

        # Get cached IRC state
        irc_state = self._get_target_irc_state()
        
        if irc_state:
            announcement = f"\x033[ PRE ]\x03 [ {section_formatted} ] {releasename}"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, announcement))
            #self.log.info(f"Message sent to {target_channel} on omg network: {announcement}")
        else:
            self.log.error("Target network omg is not connected. Cannot announce release.")

    # ========================
    # ANNOUNCE INFO TO CHANNEL
    # ========================    
    def announce_info(self, irc, releasename, files, size):
        """Announce info update to the #omgwtfnzbs.info channel on another network."""
        #target_channel = "#omgwtfnzbs.info"
        target_channel = self.registryValue('InfoannounceChannel')

        # Get cached IRC state
        irc_state = self._get_target_irc_state()
        
        if irc_state:
            # Format the announcement
            announcement = f"[ {releasename} ]"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, announcement))
            
            # Send the info line
            info_line = f"[\x033 ADDED.INFO\x03: {size} \x033MB\x03, {files} \x033Files\x03 ]"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, info_line))
        else:
            self.log.error("Target network omg is not connected. Cannot announce info.")            

    # =========================
    # ANNOUNCE GENRE TO CHANNEL
    # =========================
    def announce_genre(self, irc, releasename, genre):
        """Announce genre update to the #omgwtfnzbs.info channel on another network."""
        #target_channel = "#omgwtfnzbs.info"
        target_channel = self.registryValue('InfoannounceChannel')

        # Get cached IRC state
        irc_state = self._get_target_irc_state()
        
        if irc_state:
            # Format the announcement
            announcement = f"[ {releasename} ]"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, announcement))
            
            # Send the genre line
            genre_line = f"[ \x033ADDED GENRE\x03: {genre} ]"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, genre_line))
        else:
            self.log.error("Target network omg is not connected. Cannot announce genre.")

    # =========================
    # ANNOUNCE URL TO CHANNEL
    # =========================
    def announce_url(self, irc, releasename, url):
        """Announce URL update to the #omgwtfnzbs.info channel on another network."""
        #target_channel = "#omgwtfnzbs.info"
        target_channel = self.registryValue('InfoannounceChannel')

        # Get cached IRC state
        irc_state = self._get_target_irc_state()
        
        if irc_state:
            # Format the announcement
            announcement = f"[ {releasename} ]"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, announcement))
            
            # Send the URL line
            url_line = f"[ \x033ADDED.URL\x03: {url} ]"
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, url_line))
        else:
            self.log.error("Target network omg is not connected. Cannot announce URL.")            
    # =====================
    # UNIFIED ANNOUNCEMENTS
    # =====================
    def announce_nuke_status(self, irc, releasename, reason, nukenet, nuke_type):
        """Unified nuke announcement handler"""
        nuke_status = {
            '1': ("\x0304NUKE\x03", "\x0304"),
            '2': ("\x0303UNNUKE\x03", "\x0303"),
            '3': ("\x0305MODNUKE\x03", "\x0305"),
            '4': ("\x03055DELPRE\x03", "\x0305"),
            '5': ("\x0305UNDELPRE\x03", "\x0305")
        }
        
        # Convert nuke_type to string for dictionary lookup
        nuke_type_str = str(nuke_type)
        
        name, color = nuke_status.get(nuke_type_str, ("UNKNOWN", "05"))
        announcement = f"[ \x03{color}{name}\x03 ] {releasename} [ \x03{color}{reason}\x03 ] => \x03{color}{nukenet}\x03"
        
        if irc_state := self._get_target_irc_state():
            target_channel = self.registryValue('PreannounceChannel')
            irc_state.queueMsg(ircmsgs.privmsg(target_channel, announcement))

    # Update nuke handlers to use this unified method:
    # - In _nuke_thread: self.announce_nuke_status(irc, releasename, reason, nukenet, 1)
    # - In _unnuke_thread: self.announce_nuke_status(irc, releasename, reason, nukenet, 2)
    # - In _modnuke_thread: self.announce_nuke_status(irc, releasename, reason, nukenet, 3)

    # ========================
    # OPTIMIZED MESSAGE PROCESSING
    # ========================
    def doPrivmsg(self, irc, msg):
        """Optimized message processing"""
        text = msg.args[1]
        channel = msg.args[0]
        
        # Store for command handlers
        self.irc = irc
        self.msg = msg
            
        # Process commands in any channel
        if any(text.startswith(cmd) for cmd in self.command_handlers):
            self._process_command(text)
        else:
            self.log.debug("Message doesn't match any patterns, skipping")

        # Throttle cache cleanup to every 5 minutes instead of every message
        now = time.time()
        if now - self._last_memory_check > 300:
            self._clean_expired_cache_entries()
            self._last_memory_check = now

    def _clean_expired_cache_entries(self):
        """Clean expired entries from release cache"""
        now = time.time()
        expired_keys = [
            key for key, timestamp in self.cache_timestamps.items()
            if now - timestamp > self.cache_ttl
        ]
        for key in expired_keys:
            if key in self.release_cache:
                del self.release_cache[key]
            if key in self.cache_timestamps:
                del self.cache_timestamps[key]

    def clear_caches(self):
        """Clear all caches to free memory"""
        self.get_cached_content.cache_clear()
        self.shorten_url.cache_clear()
        self.fetch_release.cache_clear()
        self._get_db_stats_cached.cache_clear()
        
        self.nfo_cache.clear()
        self.sfv_cache.clear() 
        self.srr_cache.clear()
        self.release_cache.clear()
        self.cache_timestamps.clear()
        
        self.log.info("All caches cleared")

    # ===================
    # DATABASE STATISTICS
    # ===================
    def _get_db_stats_cached(self, cache_key):
        """Cached database statistics with optimized caching"""
        # Return cached result if same cache key
        if self._last_cache_key == cache_key and self._cached_stats:
            return self._cached_stats
        
        # Check if we need to refresh cache
        if self._last_cache_key != cache_key:
            self._cached_stats = self._get_db_stats(cache_key)
            self._last_cache_key = cache_key
        
        return self._cached_stats

    def _get_db_stats(self, cache_key):
        """Get database statistics with optimized query"""
        try:
            start_of_today = datetime.combine(date.today(), datetime_time.min).timestamp()
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS total,
                        SUM(CASE WHEN unixtime >= %s THEN 1 ELSE 0 END) AS today,
                        SUM(nuked = '1') AS nukes,
                        SUM(nuked = '2') AS unnukes,
                        SUM(nuked = '3') AS modnukes,
                        SUM(nuked = '4') AS delpres,
                        SUM(nuked = '5') AS undelpres,
                        SUM(CASE WHEN url IS NOT NULL AND url != '' THEN 1 ELSE 0 END) AS url_count,
                        (SELECT releasename FROM releases ORDER BY unixtime DESC LIMIT 1)
                    FROM releases
                """, (int(start_of_today),))
                
                result = cursor.fetchone()
                if result:
                    # Cache the successful result
                    self._cached_stats = result
                    self._last_cache_key = cache_key
                return result
                
        except Exception as e:
            self.log.error(f"Database stats error: {e}")
            return None

    def db(self, irc, msg, args):
        """Efficient stats with time-based cache invalidation"""
        cache_key = int(time.time() // 60)  # Cache for 60 seconds
        stats = self._get_db_stats_cached(cache_key)
        if stats:
            total, today, nuked, unnuked, modnuked, delpred, undelpred, url_count, last_pre = stats
            # Convert None values to 0 for counts
            total = total or 0
            today = today or 0
            nuked = nuked or 0
            delpred = delpred or 0
            undelpred = undelpred or 0
            unnuked = unnuked or 0
            modnuked = modnuked or 0
            url_count = url_count or 0

            irc.reply(
                f"[ PRE DATABASE ] [ \x033RELEASES\x03: {self.human_readable_number(total)} ] "
                f"[ \x033TODAY\x03: {self.human_readable_number(today)} ] [ \x0303URLS\x03: {self.human_readable_number(url_count)} ] [ \x0305NUKES\x03: {self.human_readable_number(nuked)} ] "
                f"[ \x033UNNUKES\x03: {self.human_readable_number(unnuked)} ] [ \x034MODNUKES\x03: {self.human_readable_number(modnuked)} ] "
                f"[ \x034DELPRES\x03: {self.human_readable_number(delpred)} ] [ \x033UNDELPRES\x03: {self.human_readable_number(undelpred)} ] "
                f"[ \x0306Last Pre\x03: {last_pre or 'None'} ]"
            )
        else:
            irc.reply("Error retrieving database stats")

    def newnukes(self, irc, msg, args, nuke_id=None):
        """[<release_id>] - List pending nukes or show details of a specific one"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                if nuke_id:
                    # Show specific nuke by ID
                    try:
                        nuke_id_int = int(nuke_id)
                        cursor.execute("""
                            SELECT id, releasename, reason, nukenet, unixtime, section, grp, files, size
                            FROM new_nukes
                            WHERE id = %s
                        """, (nuke_id_int,))
                        result = cursor.fetchone()

                        if not result:
                            irc.reply(f"Pending nuke ID {nuke_id} not found")
                            return

                        nuke_id_val, releasename, reason, nukenet, unixtime, section, grp, files, size = result
                        # REMOVED: time_ago = self.format_time_ago(unixtime)
                        
                        message = (
                            f"[ \x0305PENDING NUKE\x03 ] ID: {nuke_id_val} | {releasename} | "
                            f"Reason: {reason} | Network: {nukenet} | Time: {unixtime}"  # CHANGED: {time_ago} to {unixtime}
                        )
                        irc.reply(message, private=True)
                    except ValueError:
                        irc.reply("Invalid ID. Usage: +newnukes [<id>]")
                else:
                    # List all pending nukes
                    cursor.execute("""
                        SELECT id, releasename, reason, nukenet, unixtime
                        FROM new_nukes
                        ORDER BY unixtime DESC
                        LIMIT 20
                    """)
                    results = cursor.fetchall()

                    if not results:
                        irc.reply("No pending nukes")
                        return

                    irc.reply(f"Sending {len(results)} pending nukes to {msg.nick}")
                    
                    for nuke_id, releasename, reason, nukenet, unixtime in results:
                        # REMOVED: time_ago = self.format_time_ago(unixtime)
                        message = f"[ ID: {nuke_id} ] [ Release: {releasename} ] [ Reason: {reason} => Network: {nukenet} ] [ Unixtime: {unixtime} ]"  # CHANGED: {time_ago} to {unixtime}
                        irc.reply(message, private=True)

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    newnukes = commands.wrap(newnukes, [optional('text')])

    # ===============================================================================
    # Use this to convert timestamp to unix timestamp https://www.epochconverter.com/
    # ===============================================================================
    @admin_only
    def movenuke(self, irc, msg, args):
        """<id> <unixtime> [<section>] - Move a pending nuke to main database with timestamp"""
        
        params, error = self._validate_move_args(args, '1')
        if error:
            irc.reply(error)
            return

        nuke_id = params['id']
        new_unixtime = params['unixtime']
        section = params['section']

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Verify the pending nuke exists and is correct type
                cursor.execute("""
                    SELECT releasename, reason, nukenet, grp, nuked
                    FROM new_nukes
                    WHERE id = %s
                """, (nuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Error: Pending nuke ID {nuke_id} not found")
                    return

                releasename, reason, nukenet, grp, nuked_type = result
                
                # Verify correct type
                if nuked_type != params['expected_type']:
                    type_names = {'1': 'nuke', '2': 'unnuke', '3': 'modnuke', '4': 'delpre', '5': 'undelpre'}
                    actual_type = type_names.get(nuked_type, 'unknown')
                    irc.reply(f"Error: ID {nuke_id} is a {actual_type}, not a nuke. Use +move{actual_type} instead.")
                    return

                # Check if release already exists in main database
                cursor.execute("SELECT releasename FROM releases WHERE releasename = %s", (releasename,))
                if cursor.fetchone():
                    irc.reply(f"Error: Release '{releasename}' already exists in main database")
                    return

                # Insert into main releases table
                cursor.execute(
                    """INSERT INTO releases 
                       (releasename, section, unixtime, grp, reason, nukenet, nuked) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet, nuked_type),
                )

                if cursor.rowcount != 1:
                    conn.rollback()
                    irc.reply("Error: Failed to insert into main database")
                    return

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (nuke_id,))
                
                if cursor.rowcount != 1:
                    conn.rollback()
                    irc.reply("Error: Failed to delete from pending nukes")
                    return
                    
                conn.commit()

                safe_section = section or "UNKNOWN"
                irc.reply(f"Moved: {releasename} to main database | Section: {safe_section} | Unixtime: {new_unixtime}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # ==========
    # Deletenuke
    # ==========
    @admin_only
    def deletenuke(self, irc, msg, args):
        """<id> - Delete a pending nuke"""
        
        params, error = self._validate_delete_args(args, '1')
        if error:
            irc.reply(error)
            return

        nuke_id = params['id']

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Verify the pending nuke exists and is correct type
                cursor.execute("""
                    SELECT nuked FROM new_nukes WHERE id = %s
                """, (nuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Error: Pending nuke ID {nuke_id} not found")
                    return

                nuked_type = result[0]
                
                # Verify correct type
                if nuked_type != params['expected_type']:
                    type_names = {'1': 'nuke', '2': 'unnuke', '3': 'modnuke', '4': 'delpre', '5': 'undelpre'}
                    actual_type = type_names.get(nuked_type, 'unknown')
                    irc.reply(f"Error: ID {nuke_id} is a {actual_type}, not a nuke. Use +delete{actual_type} instead.")
                    return

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (nuke_id,))
                conn.commit()

                if cursor.rowcount > 0:
                    irc.reply(f"Deleted pending nuke ID: {nuke_id}")
                else:
                    irc.reply(f"Error: Failed to delete pending nuke ID: {nuke_id}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    deletenuke = commands.wrap(deletenuke, ['text'])

    # ===========
    # New unnukes
    # ===========
    def newunnukes(self, irc, msg, args, unnuke_id=None):
        """[<release_id>] - List pending unnukes or show details of a specific one"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                if unnuke_id:
                    # Show specific unnuke by ID
                    try:
                        unnuke_id_int = int(unnuke_id)
                        cursor.execute("""
                            SELECT id, releasename, reason, nukenet, unixtime, section, grp, files, size
                            FROM new_nukes
                            WHERE id = %s
                        """, (unnuke_id_int,))
                        result = cursor.fetchone()

                        if not result:
                            irc.reply(f"Pending unnuke ID: {unnuke_id} not found")
                            return

                        unnuke_id_val, releasename, reason, nukenet, unixtime, section, grp, files, size = result
                        # REMOVED: time_ago = self.format_time_ago(unixtime)
                        
                        message = (
                            f"[ \x0303PENDING UNNUKE\x03 ] ID: {unnuke_id_val} | {releasename} | "
                            f"Reason: {reason} | Network: {nukenet} | Time: {unixtime}"  # CHANGED: {time_ago} to {unixtime}
                        )
                        irc.reply(message, private=True)
                    except ValueError:
                        irc.reply("Invalid ID. Usage: +newunnukes [<id>]")
                else:
                    # List all pending unnukes
                    cursor.execute("""
                        SELECT id, releasename, reason, nukenet, unixtime
                        FROM new_nukes
                        ORDER BY unixtime DESC
                        LIMIT 20
                    """)
                    results = cursor.fetchall()

                    if not results:
                        irc.reply("No pending unnukes")
                        return

                    irc.reply(f"Sending {len(results)} pending unnukes to {msg.nick}")
                    
                    for unnuke_id, releasename, reason, nukenet, unixtime in results:
                        # REMOVED: time_ago = self.format_time_ago(unixtime)
                        # REMOVED: message = f"[ ID: {unnuke_id} ] [ {releasename} ] [ {reason} => {nukenet} ] [ {time_ago} ]"
                        message = f"[ ID: {unnuke_id} ] [ Release: {releasename} ] [ Reason: {reason} => Network: {nukenet} ] [ Unixtime: {unixtime} ]"  # CHANGED: {time_ago} to {unixtime}
                        irc.reply(message, private=True)

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    newunnukes = commands.wrap(newunnukes, [optional('text')])

    # ============
    # Move Unnukes
    # ============
    @admin_only
    def moveunnuke(self, irc, msg, args):
        """<id> <unixtime> [<section>] - Move a pending unnuke to main database with timestamp"""
        
        params, error = self._validate_move_args(args, '2')
        if error:
            irc.reply(error)
            return

        unnuke_id = params['id']
        new_unixtime = params['unixtime']
        section = params['section']

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Verify the pending unnuke exists and is correct type
                cursor.execute("""
                    SELECT releasename, reason, nukenet, grp, nuked
                    FROM new_nukes
                    WHERE id = %s
                """, (unnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Error: Pending unnuke ID {unnuke_id} not found")
                    return

                releasename, reason, nukenet, grp, nuked_type = result
                
                # Verify correct type (should be '2' for unnuke)
                if nuked_type != params['expected_type']:
                    type_names = {'1': 'nuke', '2': 'unnuke', '3': 'modnuke', '4': 'delpre', '5': 'undelpre'}
                    actual_type = type_names.get(nuked_type, 'unknown')
                    irc.reply(f"Error: ID {unnuke_id} is a {actual_type}, not an unnuke. Use +move{actual_type} instead.")
                    return

                # Check if release already exists in main database
                cursor.execute("SELECT releasename FROM releases WHERE releasename = %s", (releasename,))
                if cursor.fetchone():
                    irc.reply(f"Error: Release '{releasename}' already exists in main database")
                    return

                # Insert into main releases table (nuked=2 for unnuke)
                cursor.execute(
                    """INSERT INTO releases 
                       (releasename, section, unixtime, grp, reason, nukenet, nuked) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet, nuked_type),
                )

                if cursor.rowcount != 1:
                    conn.rollback()
                    irc.reply("Error: Failed to insert into main database")
                    return

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (unnuke_id,))
                
                if cursor.rowcount != 1:
                    conn.rollback()
                    irc.reply("Error: Failed to delete from pending unnukes")
                    return
                    
                conn.commit()

                safe_section = section or "UNKNOWN"
                irc.reply(f"Moved: {releasename} to main database | Section: {safe_section} | Unixtime: {new_unixtime}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    # ===============
    # Delete Unnukes
    # ===============
    def deleteunnuke(self, irc, msg, args):
        """<id> - Delete a pending unnuke"""
        
        params, error = self._validate_delete_args(args, '2')
        if error:
            irc.reply(error)
            return

        unnuke_id = params['id']

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Verify the pending unnuke exists and is correct type
                cursor.execute("SELECT nuked FROM new_nukes WHERE id = %s", (unnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Error: Pending unnuke ID {unnuke_id} not found")
                    return

                nuked_type = result[0]
                
                # Verify correct type
                if nuked_type != params['expected_type']:
                    type_names = {'1': 'nuke', '2': 'unnuke', '3': 'modnuke', '4': 'delpre', '5': 'undelpre'}
                    actual_type = type_names.get(nuked_type, 'unknown')
                    irc.reply(f"Error: ID {unnuke_id} is a {actual_type}, not an unnuke. Use +delete{actual_type} instead.")
                    return

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (unnuke_id,))
                conn.commit()

                if cursor.rowcount > 0:
                    irc.reply(f"Deleted pending unnuke ID: {unnuke_id}")
                else:
                    irc.reply(f"Error: Failed to delete pending unnuke ID: {unnuke_id}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error in deleteunnuke: {e}")
            irc.reply(f"Database error: {e.msg}")
        except Exception as e:
            self.log.error(f"Unexpected error in deleteunnuke: {e}")
            irc.reply("An unexpected error occurred")

    deleteunnuke = commands.wrap(deleteunnuke, ['text'])

# =============
# New  Modnukes
# =============
    def newmodnukes(self, irc, msg, args, modnuke_id=None):
        """[<release_id>] - List pending modnukes or show details of a specific one"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                if modnuke_id:
                    # Show specific nuke by ID
                    try:
                        modnuke_id_int = int(modnuke_id)
                        cursor.execute("""
                            SELECT id, releasename, reason, nukenet, unixtime, section, grp, files, size
                            FROM new_nukes
                            WHERE id = %s
                        """, (modnuke_id_int,))
                        result = cursor.fetchone()

                        if not result:
                            irc.reply(f"Pending modnuke ID: {modnuke_id} not found")
                            return

                        modnuke_id_val, releasename, reason, nukenet, unixtime, section, grp, files, size = result
                        # REMOVED: time_ago = self.format_time_ago(unixtime)
                        
                        message = (
                            f"[ \x0305PENDING MODNUKE\x03 ] ID: {modnuke_id_val} | {releasename} | "
                            f"Reason: {reason} | Network: {nukenet} | Time: {unixtime}"  # CHANGED: {time_ago} to {unixtime}
                        )
                        irc.reply(message, private=True)
                    except ValueError:
                        irc.reply("Invalid ID. Usage: +newmodnukes [<id>]")
                else:
                    # List all pending modnukes
                    cursor.execute("""
                        SELECT id, releasename, reason, nukenet, unixtime
                        FROM new_nukes
                        ORDER BY unixtime DESC
                        LIMIT 20
                    """)
                    results = cursor.fetchall()

                    if not results:
                        irc.reply("No pending modnukes")
                        return

                    irc.reply(f"Sending {len(results)} pending modnukes to {msg.nick}")
                    
                    for modnuke_id, releasename, reason, nukenet, unixtime in results:
                        # REMOVED: time_ago = self.format_time_ago(unixtime)
                        # REMOVED: message = f"[ ID: {nuke_id} ] [ {releasename} ] [ {reason} => {nukenet} ] [ {time_ago} ]"
                        message = f"[ ID: {modnuke_id} ] [ Release: {releasename} ] [ Reason: {reason} => Network: {nukenet} ] [ Unixtime: {unixtime} ]"  # CHANGED: {time_ago} to {unixtime}
                        irc.reply(message, private=True)

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    newmodnukes = commands.wrap(newmodnukes, [optional('text')])

# ==============
# Move  Modnukes
# ==============
    @admin_only
    def movemodnuke(self, irc, msg, args):
        """<id> <unixtime> [<section>] - Move a pending modnuke to main database with timestamp"""
        
        params, error = self._validate_move_args(args, '3')
        if error:
            irc.reply(error)
            return

        modnuke_id = params['id']
        new_unixtime = params['unixtime']
        section = params['section']

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Verify the pending modnuke exists and is correct type
                cursor.execute("""
                    SELECT releasename, reason, nukenet, grp, nuked
                    FROM new_nukes
                    WHERE id = %s
                """, (modnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Error: Pending modnuke ID {modnuke_id} not found")
                    return

                releasename, reason, nukenet, grp, nuked_type = result
                
                # Verify correct type
                if nuked_type != params['expected_type']:
                    type_names = {'1': 'nuke', '2': 'unnuke', '3': 'modnuke', '4': 'delpre', '5': 'undelpre'}
                    actual_type = type_names.get(nuked_type, 'unknown')
                    irc.reply(f"Error: ID {modnuke_id} is a {actual_type}, not a modnuke. Use +move{actual_type} instead.")
                    return

                # Check if release already exists in main database
                cursor.execute("SELECT releasename FROM releases WHERE releasename = %s", (releasename,))
                if cursor.fetchone():
                    irc.reply(f"Error: Release '{releasename}' already exists in main database")
                    return

                # Insert into main releases table
                cursor.execute(
                    """INSERT INTO releases 
                       (releasename, section, unixtime, grp, reason, nukenet, nuked) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet, nuked_type),
                )

                if cursor.rowcount != 1:
                    conn.rollback()
                    irc.reply("Error: Failed to insert into main database")
                    return

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (modnuke_id,))
                
                if cursor.rowcount != 1:
                    conn.rollback()
                    irc.reply("Error: Failed to delete from pending modnukes")
                    return
                    
                conn.commit()

                safe_section = section or "UNKNOWN"
                irc.reply(f"Moved: {releasename} to main database | Section: {safe_section} | Unixtime: {new_unixtime}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

# =================
# Delete  Modnukes
# =================
    def deletemodnukes(self, irc, msg, args):
        """<id> - Delete a pending modnuke"""
        
        params, error = self._validate_delete_args(args, '3')
        if error:
            irc.reply(error)
            return

        modnuke_id = params['id']

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Verify the pending modnuke exists and is correct type
                cursor.execute("SELECT nuked FROM new_nukes WHERE id = %s", (modnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Error: Pending modnuke ID {modnuke_id} not found")
                    return

                nuked_type = result[0]
                
                # Verify correct type
                if nuked_type != params['expected_type']:
                    type_names = {'1': 'nuke', '2': 'unnuke', '3': 'modnuke', '4': 'delpre', '5': 'undelpre'}
                    actual_type = type_names.get(nuked_type, 'unknown')
                    irc.reply(f"Error: ID {modnuke_id} is a {actual_type}, not a modnuke. Use +delete{actual_type} instead.")
                    return

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (modnuke_id,))
                conn.commit()

                if cursor.rowcount > 0:
                    irc.reply(f"Deleted pending modnuke ID: {modnuke_id}")
                else:
                    irc.reply(f"Error: Failed to delete pending modnuke ID: {modnuke_id}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error in deletemodnukes: {e}")
            irc.reply(f"Database error: {e.msg}")
        except Exception as e:
            self.log.error(f"Unexpected error in deletemodnukes: {e}")
            irc.reply("An unexpected error occurred")

    deletemodnukes = commands.wrap(deletemodnukes, ['text'])

# =======
# Pending
# =======
    def pending(self, irc, msg, args):
        """Show counts of pending nukes, unnukes, and modnukes"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Count each type based on the nuked column
                cursor.execute("SELECT nuked, COUNT(*) FROM new_nukes GROUP BY nuked")
                results = cursor.fetchall()
                
                # Initialize counts - using string keys to match ENUM values
                counts = {'1': 0, '2': 0, '3': 0, '4': 0, '5': 0}
                for nuked_type, count in results:
                    if nuked_type in counts:
                        counts[nuked_type] = count
                    else:
                        # Handle any unexpected values
                        self.log.warning(f"Unexpected nuked value: {nuked_type}")
                
                irc.reply(f"\x0305Nukes\x03: {counts['1']} | \x0303Unnukes\x03: {counts['2']} | \x0305Modnukes\x03: {counts['3']}")

        except mysql.connector.Error as e:
            self.log.error(f"Database error {e.errno}: {e.msg}")
            irc.reply("Database error occurred")
        except ValueError as e:
            irc.reply(f"Invalid input: {e}")

    pending = commands.wrap(pending, [])
    
    # ================
    # HELPER FUNCTIONS
    # ================
    def _get_target_irc_state(self):
        """Cached IRC state lookup with connection validation"""
        # Check if cached state is still valid and connected
        if self.target_irc_state and self.target_irc_state in world.ircs:
            # Verify the connection is actually active
            if self._is_irc_connected(self.target_irc_state):
                return self.target_irc_state
            else:
                # Cached state is disconnected, clear it and search again
                self.log.debug("Cached IRC state is disconnected, searching for new connection")
                self.target_irc_state = None
        
        # Search for active connection to target network
        for irc_state in world.ircs:
            if "omg" in irc_state.network and self._is_irc_connected(irc_state):
                self.target_irc_state = irc_state
                self.log.info(f"Found active IRC connection to {irc_state.network}")
                return irc_state
        
        self.log.warning("No active IRC connection found for target network 'omg'")
        return None
    
    def _is_irc_connected(self, irc_state):
        """Check if an IRC state object is actually connected"""
        try:
            # Check if driver exists and is connected
            if hasattr(irc_state, 'driver') and irc_state.driver:
                if hasattr(irc_state.driver, 'connected') and irc_state.driver.connected:
                    return True
            
            # Alternative: check if zombie flag is False (disconnected clients are marked as zombies)
            if hasattr(irc_state, 'zombie') and not irc_state.zombie:
                return True
            
            # Fallback: if we have a driver object, assume connected
            if hasattr(irc_state, 'driver') and irc_state.driver is not None:
                return True
                
        except Exception as e:
            self.log.error(f"Error checking IRC connection state: {e}")
        
        return False
    
    def _is_music_release(self, releasename):
        """Check if a release is a music release by looking up its section"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT section FROM releases WHERE releasename = %s",
                    (releasename,)
                )
                result = cursor.fetchone()
                if result:
                    section = result[0]
                    return section in self.MUSIC_SECTIONS
        except Exception as e:
            self.log.error(f"Error checking if music release: {e}")
        return False    
    
    def _parse_music_release(self, releasename):
        """
        Parse a music release name to extract artist and album.
        
        Examples:
        - Wreckless-Unforced_Rhythms-(DISWRLP001BP)-16BIT-WEB-FLAC-2026-PTC
        -> artist: Wreckless, album: Unforced Rhythms, year: 2026
        - Artist_Name-Album_Title-WEB-FLAC-2024-GROUP
        -> artist: Artist Name, album: Album Title, year: 2024
        - VA-Compilation_Name-WEB-FLAC-2024-GROUP
        -> artist: VA, album: Compilation Name, year: 2024
        
        Returns:
            tuple: (artist, album, year) or (None, None, None) if parsing fails
        """
        try:
            # Extract year from release name (4 digits)
            year_match = re.search(r'-(\d{4})-[^-]+$', releasename)
            year = year_match.group(1) if year_match else None
            
            # Split by first hyphen to separate artist from rest
            parts = releasename.split('-', 1)
            if len(parts) < 2:
                return None, None, year
            
            artist = parts[0].replace('_', ' ').strip()
            
            # Extract album title (everything before format tags)
            rest = parts[1]
            
            # Remove catalog numbers in parentheses like (DISWRLP001BP)
            rest = re.sub(r'\([A-Z0-9]+\)', '', rest)
            
            # Find where the format/encoding info starts
            # Common patterns: -WEB-, -CD-, -VINYL-, -16BIT-, -24BIT-, -FLAC-, -MP3-, etc.
            format_patterns = [
                r'-(WEB|CD|VINYL|RETAIL|16BIT|24BIT|FLAC|MP3|320|V0|V2|REMASTERED|DELUXE)',
                r'-\d+BIT-',  # Catch 16BIT, 24BIT, etc.
            ]
            
            album = rest
            for pattern in format_patterns:
                format_match = re.search(pattern, rest, re.IGNORECASE)
                if format_match:
                    album = rest[:format_match.start()]
                    break
            
            # If still no match, try to find year and take everything before it
            if album == rest and year_match:
                album = rest[:year_match.start()].rstrip('-')
            
            # If still no luck, take everything except last 2 segments (usually GROUP and format)
            if album == rest:
                album_parts = rest.split('-')
                if len(album_parts) > 2:
                    album = '-'.join(album_parts[:-2])
            
            album = album.replace('_', ' ').strip().strip('-')
            
            # Clean up extra whitespace
            artist = ' '.join(artist.split())
            album = ' '.join(album.split())
            
            return artist, album, year
            
        except Exception as e:
            self.log.error(f"Error parsing release name '{releasename}': {e}")
            return None, None, None   

    def _search_discogs(self, artist, album, year=None):
        """
        Search Discogs using authenticated API and return the URL of the best match.
        
        Args:
            artist: Artist name
            album: Album title
            year: Release year (optional)
        
        Returns:
            tuple: (url, title) or (None, None) if not found
        """
        try:
            # Check if API token is available
            if not self.discogs_token:
                self.log.warning("Discogs API token not configured - skipping lookup")
                return None, None
            
            # Construct search query
            query_parts = []
            if artist:
                query_parts.append(artist)
            if album:
                query_parts.append(album)
            if year:
                query_parts.append(str(year))
            
            query = ' '.join(query_parts)
            
            if not query:
                self.log.warning("Empty search query for Discogs")
                return None, None
            
            # APPLY RATE LIMITING HERE
            self.discogs_limiter.wait_if_needed()            
            
            self.log.info(f"Searching Discogs API for: {query}")
            
            # Discogs API endpoint
            api_url = "https://api.discogs.com/database/search"
            params = {
                'q': query,
                'type': 'release',  # Search only releases (not masters, artists, labels)
                'per_page': 5,      # Get top 5 results
                'page': 1,
                'token': self.discogs_token  # Authentication token
            }
            
            headers = {
                'User-Agent': 'PreDBSQL/1.0',  # Required by Discogs
                'Accept': 'application/json'
            }
            
            response = self.session.get(api_url, params=params, headers=headers, timeout=15)
            
            # Handle rate limiting (fallback if limiter fails)
            if response.status_code == 429:
                self.log.warning("Discogs API rate limit hit (429) - backing off")
                time.sleep(5)
                return None, None
           
            if response.status_code == 401:
                self.log.error("Discogs API authentication failed - check your token")
                return None, None
            
            if response.status_code != 200:
                self.log.warning(f"Discogs API returned status {response.status_code}")
                return None, None
            
            data = response.json()
            
            # Check if we got results
            results = data.get('results', [])
            if not results:
                self.log.info(f"No Discogs results found for: {query}")
                return None, None
            
            # Get the first result
            first_result = results[0]
            
            # Log all results for debugging
            self.log.debug(f"Found {len(results)} Discogs results:")
            for i, result in enumerate(results[:3]):
                self.log.debug(f"  {i+1}. {result.get('title', 'N/A')} ({result.get('year', 'N/A')})")
            
            # Build the web URL from the result
            # Discogs returns:
            # - uri: "/release/36492040-Aaron-Schwarz-2-Parallel-Motion" (best option)
            # - resource_url: "https://api.discogs.com/releases/36492040" (API endpoint)
            
            uri = first_result.get('uri')
            if uri:
                # This is the cleanest way - uri is the web path
                full_url = f"https://www.discogs.com{uri}"
            else:
                # Fallback: construct from resource_url
                resource_url = first_result.get('resource_url', '')
                if '/releases/' in resource_url:
                    release_id = resource_url.split('/releases/')[-1]
                    # Try to construct a clean URL with slug
                    title = first_result.get('title', '')
                    if title:
                        # Clean title for URL slug
                        slug = title.replace(' - ', '-').replace(' ', '-')
                        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
                        full_url = f"https://www.discogs.com/release/{release_id}-{slug}"
                    else:
                        full_url = f"https://www.discogs.com/release/{release_id}"
                elif '/masters/' in resource_url:
                    master_id = resource_url.split('/masters/')[-1]
                    full_url = f"https://www.discogs.com/master/{master_id}"
                else:
                    self.log.warning(f"Could not parse Discogs resource URL: {resource_url}")
                    return None, None
            
            # Extract title and other metadata
            title = first_result.get('title', None)
            result_year = first_result.get('year', None)
            formats = first_result.get('format', [])
            
            self.log.info(f"Found Discogs release: {title} ({result_year})")
            self.log.info(f"Discogs URL: {full_url}")
            if formats:
                self.log.debug(f"Format: {', '.join(formats)}")
            
            return full_url, title
            
        except requests.RequestException as e:
            self.log.error(f"Network error searching Discogs API: {e}")
            return None, None
        except json.JSONDecodeError as e:
            self.log.error(f"Error parsing Discogs API JSON response: {e}")
            return None, None
        except json.JSONDecodeError as e:
            self.log.error(f"Unexpected error in Discogs API search: {e}", exc_info=True)
            return None, None

    def _update_release_url(self, releasename, url):
        """
        Update the URL field for a release in the database.
        
        Args:
            releasename: Release name to update
            url: Discogs URL to store
        
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET url = %s WHERE releasename = %s",
                    (url, releasename)
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    self.log.info(f"Updated URL for {releasename}: {url}")
                    return True
                else:
                    self.log.warning(f"No rows updated for {releasename}")
                    return False
                    
        except Exception as e:
            self.log.error(f"Error updating release URL in database: {e}")
            return False


    def _process_discogs_lookup(self, releasename, announce_irc=None):
        """
        Process a complete Discogs lookup: parse, search, and update database.
        
        Args:
            releasename: Full scene release name
            announce_irc: IRC object for announcements (optional)
        
        Returns:
            tuple: (url, message) - URL if successful (or None), and status message
        """
        # Parse the release name
        artist, album, year = self._parse_music_release(releasename)
        
        if not artist or not album:
            msg = f"Could not parse release name: {releasename}"
            self.log.warning(msg)
            return None, msg
        
        self.log.info(f"Parsed: Artist='{artist}', Album='{album}', Year='{year}'")
        
        # Search Discogs
        url, title = self._search_discogs(artist, album, year)
        
        if not url:
            msg = f"No Discogs results for: {artist} - {album}"
            if year:
                msg += f" ({year})"
            return None, msg
        
        # Update database
        success = self._update_release_url(releasename, url)
        
        if success:
            msg = f"✓ {artist} - {album}"
            if year:
                msg += f" ({year})"
            
            # ANNOUNCE USING YOUR EXISTING METHOD
            if announce_irc:
                self.announce_url(announce_irc, releasename, url)
            
            return url, msg
        else:
            return None, "Failed to update database"

    def human_readable_number(self, n):
        """Convert large numbers into a human-readable format with commas (e.g., 12,345,678)."""
        if n is None:
            return "0"
        return f"{n:,}"

    def _validate_move_args(self, args, expected_nuked_type):
        """Validate arguments for move commands (movenuke, moveunnuke, movemodnuke)"""
                
        if not isinstance(args, list) or len(args) < 2:
            return None, "Usage: +move<command> <id> <unixtime> [<section>]"
        
        # Validate ID
        try:
            record_id = int(args[0])
            if record_id <= 0:
                return None, "Error: ID must be a positive integer"
        except ValueError:
            return None, "Error: ID must be a valid integer"
        
        # Validate unixtime
        try:
            unixtime = int(args[1])
            current = int(time.time())
            if unixtime > current + 3600:
                return None, "Error: Unixtime cannot be more than 1 hour in the future"
            if unixtime < 946684800:  # Jan 1, 2000
                return None, "Error: Unixtime is too old (before year 2000)"
        except ValueError:
            return None, "Error: Unixtime must be a valid integer"
        
        # Validate section
        section = None
        if len(args) > 2:
            section = ' '.join(args[2:]).strip()
            if not re.match(r'^[a-zA-Z0-9\s\-_]+$', section):
                return None, "Error: Section contains invalid characters (allowed: letters, numbers, spaces, hyphens, underscores)"
            section = section[:50].upper()
        
        return {
            'id': record_id,
            'unixtime': unixtime,
            'section': section,
            'expected_type': expected_nuked_type
        }, None
    
    def _validate_delete_args(self, args, expected_nuked_type):
        """Validate arguments for delete commands (deletenuke, deleteunnuke, deletemodnuke)"""
        if not isinstance(args, list) or len(args) < 1:
            return None, "Usage: +delete<command> <id>"
        
        # Validate ID
        try:
            record_id = int(args[0])
            if record_id <= 0:
                return None, "Error: ID must be a positive integer"
        except ValueError:
            return None, "Error: ID must be a valid integer"
        
        return {
            'id': record_id,
            'expected_type': expected_nuked_type
        }, None

    # ============================================
    # UNIFIED NUKE HANDLERS (Replaces duplicates)
    # ============================================
    
    def _handle_nuke_operation(self, irc, releasename, reason, nukenet, 
                               nuke_type, check_value, name, pending_cmd):
        """Unified handler for all nuke operations"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Check if release exists in main releases table
                cursor.execute("SELECT nuked FROM releases WHERE releasename = %s", (releasename,))
                result = cursor.fetchone()

                if result:
                    # Release exists - check if already in target state
                    if result[0] == check_value:
                        irc.reply(f"Release {releasename} is already {name}d.")
                        return

                    # Update the release
                    cursor.execute(
                        "UPDATE releases SET nuked = %s, reason = %s, nukenet = %s WHERE releasename = %s",
                        (nuke_type, reason, nukenet, releasename),
                    )
                    conn.commit()

                    if cursor.rowcount > 0:
                        self.announce_nuke_status(irc, releasename, reason, nukenet, int(nuke_type))
                else:
                    # Release not found - add to new_nukes instead
                    group = releasename.split('-')[-1] if '-' in releasename else None
                    
                    try:
                        cursor.execute(
                            "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (%s, %s, %s, %s, %s)",
                            (releasename, reason, nukenet, group, nuke_type),
                        )
                        conn.commit()
                        irc.reply(f"Release {releasename} not in database - added to pending {name}s. Use +{pending_cmd} to review.")
                    except mysql.connector.IntegrityError:
                        irc.reply(f"Release {releasename} already in pending {name}s")
                    except Exception as e:
                        self.log.error(f"Error adding to new_nukes: {e}")
                        irc.reply(f"Error adding to pending {name}s")

        except Exception as e:
            self.log.error(f"{name} error: {e}")
            irc.reply(f"Error processing {name}: {str(e)}")

    def _dispatch_nuke_thread(self, irc, operation, releasename, reason, nukenet):
        """Dispatch to unified handler based on operation type"""
        config = self.nuke_handlers.get(operation)
        if not config:
            self.log.error(f"Unknown nuke operation: {operation}")
            return
            
        self._handle_nuke_operation(
            irc, releasename, reason, nukenet,
            nuke_type=config['type'],
            check_value=config['check'],
            name=config['name'],
            pending_cmd=config['pending_cmd']
        )

    def die(self):
        """Called when plugin is unloaded - cleanup resources"""
        self.log.info("PreDBSQL plugin shutting down, cleaning up resources...")
        
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=True, cancel_futures=False)
        
        if hasattr(self, 'link_pool'):
            self.link_pool.shutdown(wait=True, cancel_futures=False)
        
        if hasattr(self, 'session'):
            self.session.close()
        
        self.log.info("PreDBSQL plugin cleanup complete")
        
    def prehelp(self, irc, msg, args):
        """Sends help information about the Kudos plugin in a private message."""
        # Announce in channel if command was used publicly
        if irc.isChannel(msg.args[0]):
            irc.reply(f"sending prehelp to {msg.nick} in PM", private=False)
        
        # Send help content via PM
        help_messages = [
            "\x02\x1f:: PREHELP ::\x1f\x02",
            " ",  # Empty line for spacing
            "+pre <releasename>: To search for one specific result.",
            "+dupe <part of the releasename>: Last 10 in private",
            "+group <group name>: Shows group stats of the specified group, in private",
            "+lastnuke <group>: Shows last nuked release in private (Groupname is optional).",
            "+lastunnuke <group>: Shows last unnuked release in private (Groupname is optional).",
            "+lastmodnuke <group>: Shows last unnuked release in private (Groupname is optional).",
            "+section <section> : Shows last 10 releases in the selected section",
            "+db : Shows statistics of the DataBase.",
        ]
        for message in help_messages:
            irc.reply(message, to=msg.nick, private=True, notice=False)


Class = PreDBSQL