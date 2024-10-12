###
# Copyright (c) 2015, butterscotchstallion
# Copyright (c) 2020, oddluck <oddluck@riseup.net>
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

import supybot.utils as utils
from supybot.commands import *
import supybot.plugins as plugins
import supybot.ircutils as ircutils
import supybot.ircmsgs as ircmsgs
import supybot.callbacks as callbacks
import supybot.log as log
from string import Template
from urllib.parse import quote
import datetime, time, json, re

try:
    from supybot.i18n import PluginInternationalization

    _ = PluginInternationalization("OMG")
except ImportError:
    _ = lambda x: x

class OMG(callbacks.Plugin):
    """Queries OMG website for information about releases"""

    threaded = True

    def dosearch(self, query, channel):
        user = self.registryValue("user")
        api_key = self.registryValue("apiKey")
        # Properly encode the query to handle spaces
        encoded_query = quote(query)  
        api_url = f"https://api.omgwtfnzbs.org/json/?search={encoded_query}&user={user}&nukes=1&api={api_key}"
        try:
            log.debug("OMG: requesting %s" % (api_url))
            request = utils.web.getUrl(api_url).decode()
            response = json.loads(request)
            if isinstance(response, list) and response:
                return response[0]  # Take the first result from the list
            else:
                log.error(f"OMG: No valid results found in response: {response}")
        except Exception as e:
            log.error(f"OMG: Error retrieving data from API - {e}")
        return None
      
    def get_release_info(self, result):
        release = result.get("release", "N/A")
        sizebytes = result.get("sizebytes", "N/A")
        cattext = result.get("cattext", "N/A")
        details = result.get("details", "N/A")
        usenetage = result.get("usenetage", "N/A")
        nuked = result.get("nuked", None)  # Get the nuked status, default to None

        # Define a mapping for category text
        category_mapping = {
            "apps.pc": "\x0307Apps: PC\x03",
            "apps.mac": "\x0307Apps: MAC\x03",
            "apps.phone": "\x0307Apps: Phone\x03",
            "apps.other": "\x0307Apps: Other\x03",
          
            "music.mp3": "\x0306Music: MP3\x03",
            "music.video": "\x0306Music: MViD\x03",
            "music.flac": "\x0306Music: FLAC\x03",
            "music.other": "\x0306Music: Other\x03",
           
            "other.audiobook": "\x0304Other: Audiobook\x03",
            "games.pc": "\x0302Games: PC\x03",
            "games.mac": "\x0302Games: MAC\x03",
            "games.other": "\x0302Games: Other\x03",
            
            "movies.sd": "\x0308Movies: SD\x03",
            "movies.hd": "\x0308Movies: HD\x03",
            "movies.uhd": "\x0308Movies: UHD\x03",
            "movies.full.br": "\x0308Movies: Full BR\x03",
            "movies.dvd": "\x0308Movies: DVD\x03",
            "movies.other": "\x0310Movies: Other\x03",
            
            "tv.sd": "\x0310TV: SD\x03",
            "tv.hd": "\x0310TV: HD\x03",
            "tv.uhd": "\x0310TV: UHD\x03",
            "tv.other": "\x0310TV: Other\x03",

            "xxx.sd.clips": "\x0313XXX: SD-CLiPS\x03",
            "xxx.hd.clips": "\x0313XXX: HD-CLiPS\x03",
            "xxx.uhd.clips": "\x0313XXX: UHD-CLiPS\x03",
            "xxx.movies.sd": "\x0313XXX: MOViES-SD\x03",
            "xxx.movies.hd": "\x0313XXX: MOViES-HD\x03",
            "xxx.movies.uhd": "\x0313XXX: MOViES-UHD\x03",
            "xxx.imagesets": "\x0313XXX: IMAGESETS\x03",
            "xxx.trans": "\x0313XXX: Trans\x03",
            "xxx.gay": "\x0313XXX: Gay\x03",
            "xxx.vr": "\x0313XXX: VR\x03",
            "xxx.camrips": "\x0313XXX: CamRips\x03",
            "xxx.dvd": "\x0313XXX: DVD\x03",
            "xxx.pack-other": "\x0313XXX Packs/Other\x03",
            
            "other.ebook": "\x0303Other: E-Books\x03",
            "other.other": "\x0303Other: Other\x03",
        }

        # Map the cattext to a more user-friendly format
        cattext = category_mapping.get(cattext, cattext)   
            
        # Convert sizebytes to appropriate size string
        if sizebytes != "N/A":
            try:
                sizebytes = float(sizebytes)
                if sizebytes >= 1024 ** 3:
                    size_str = f"{sizebytes / (1024 ** 3):.2f} GB"
                else:
                    size_str = f"{sizebytes / (1024 ** 2):.2f} MB"
            except ValueError:
                size_str = "N/A"
        else:
            size_str = "N/A"

        # Convert usenetage from epoch time to days ago
        if usenetage != "N/A":
            try:
                current_time = datetime.datetime.utcnow()
                usenetage_time = datetime.datetime.utcfromtimestamp(int(usenetage))
                difference = current_time - usenetage_time
                seconds = difference.total_seconds()
                
                if seconds < 3600:
                    minutes = seconds // 60
                    formatted_time = f"{int(minutes)} minutes ago"
                elif seconds < 86400:
                    hours = seconds // 3600
                    formatted_time = f"{int(hours)} hours ago"
                else:
                    days = seconds // 86400
                    formatted_time = f"{int(days)} days ago"
            except (ValueError, OverflowError):
                formatted_time = "N/A"
        else:
            formatted_time = "N/A"    

        release_info = {
            "release": release,
            "sizebytes": size_str,
            "cattext": cattext,
            "details": details,
            "usenetage": formatted_time,
            "nuked": "",  # Initialize nuked field as empty
        }

        # Only add nuked status if it is not None and has a meaningful value
        if nuked and nuked.strip():
            # Remove "1:" from the nuked status and strip unnecessary spaces
            nuked_formatted = nuked.split(':', 1)[-1].strip()
            if nuked_formatted:
                release_info["nuked"] = f"\x035Nuked: {nuked_formatted}\x03" 

        return release_info        

    def ns(self, irc, msg, args, query):
        """<search term>
        Search for omg items
        """
        user = self.registryValue("user")
        api_key = self.registryValue("apiKey")
        if not user or not api_key:
            irc.reply("Error: You need to set a user and API key to use this plugin.")
            return
        template = self.registryValue("template", msg.channel)
        template = template.replace("{{", "$").replace("}}", "")
        template = Template(template)
        result = self.dosearch(query, msg.channel)
        if result:
            log.debug("OMG: got result: %s" % result)
            show_info = self.get_release_info(result)
            # Retrieve the logo value from the configuration
            logo = self.registryValue("logo", msg.channel)
            show_info["logo"] = logo
            title = template.safe_substitute(show_info)
        else:
            irc.reply(f"No results found for: {query}")
            return
        if title:
            irc.reply(title, prefixNick=False) 

    omg = wrap(ns, ["text"])
    # Use a separate handler or different method for public messages

Class = OMG
# vim:set shiftwidth=4 softtabstop=4 expandtab textwidth=79:
