import sqlite3
import time
import threading
import sqlcipher3 as sqlcipher
import os
import requests
from datetime import datetime, date, time as datetime_time
from functools import lru_cache
from supybot.commands import wrap, optional
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import supybot.callbacks as callbacks
import supybot.ircmsgs as ircmsgs
import supybot.commands as commands
import supybot.world as world
import contextlib  # For better connection management
from collections import OrderedDict

class PreDB(callbacks.Plugin):
    """Tracks pre database entries and announces them."""

        # # Map the section to its colorized string
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
        "FLAC": "\00310FLAC\003",
        "ABOOK": "\00310ABOOK\003",
        "MVID": "\00310MVID\003",        
        "MViD": "\00310MVID\003",  
        "MDVDR": "\00310MDVDR\003",
        "MBLURAY": "\00310MBLURAY\003",            
# TV
        "TV-UHD-PL": "\00308TV-UHD-PL\003",
        "TV-UHD-DE": "\00308TV-UHD-DE\003",
        "TV-UHD-IT": "\00308TV-UHD-IT\003",
        "TV-UHD-FR": "\00308TV-UHD-FR\003",
        "TV-UHD-CZ": "\00308TV-UHD-CZ\003",
        "TV-UHD-NL": "\00308TV-UHD-NL\003",

        "TV-HD-NL": "\00308TV-HD-NL\003",
        "TV-HD-IT": "\00308TV-HD-IT\003",
        "TV-HD-PL": "\00308TV-HD-PL\003",
        "TV-HD-FR": "\00308TV-HD-FR\003",
        "TV-HD-DE": "\00308TV-HD-DE\003",
        "TV-HD-CZ": "\00308TV-HD-CZ\003",
        "TV-HD-SP": "\00308TV-HD-SP\003",
        "TV-HD-ES": "\00308TV-HD-ES\003",
        "TV-HD-NL": "\00308TV-HD-NL\003",

        "TV-SD-NL": "\00308TV-SD-NL\003",
        "TV-SD-IT": "\00308TV-SD-IT\003",
        "TV-SD-PL": "\00308TV-SD-PL\003",
        "TV-SD-FR": "\00308TV-SD-FR\003",
        "TV-SD-DE": "\00308TV-SD-DE\003",
        "TV-SD-CZ": "\00308TV-SD-CZ\003",
        "TV-SD-SP": "\00308TV-SD-SP\003",
        "TV-SD-ES": "\00308TV-SD-ES\003",
        "TV-SD-NL": "\00308TV-SD-NL\003",

        "TV": "\00308TV\003",
        "TV-XVID": "\00308TV--XVID\003",        
        "TV-SDRiP": "\00308TV-SDRiP\003",
        "TV-SD": "\00308TV-SD\003",
        "TV-UHD": "\00308TV-UHD\003",
        "TV-UHDRiP": "\00308TV-UHDRiP\003",
        "TV-HD": "\00308TV-HD\003",
        "TV-HDRiP": "\00308TV-HDRiP\003",
        "TV-HD-NORDiC": "\00308TV-HD-NORDiC\003",
        "TV-UHD-NORDiC": "\00308TV-UHD-NORDiC\003",
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
        "X264-UHD": "\00311X264-UHD\003",   
        "X264-UHD-NL": "\00311X264-UHD-NL\003",
        "X264-UHD-IT": "\00311X264-UHD-IT\003",  
        "X264-UHD-PL": "\00311X264-UHD-PL\003",
        "X264-UHD-FR": "\00311X264-UHD-FR\003",
        "X264-UHD-DE": "\00311X264-UHD-DE\003",
        "X264-UHD-CZ": "\00311X264-UHD-CZ\003",
        "X264-UHD-ES": "\00311X264-UHD-ES\003",        
        "X264-UHD-SP": "\00311X264-UHD-SP\003",
        "X264-UHD-ES": "\00311X264-UHD-ES\003",   
        "X264-UHD-NORDiC": "\00311X265-UHD-NORDiC\003",  

        "X264": "\00311X264\003",
        "X264-SD": "\00311X264-SD\003",        
        "X264-HD": "\00311X264-HD\003",
        "X264-HD-NL": "\00311X264-HD-NL\003",
        "X264-HD-IT": "\00311X264-HD-IT\003",
        "X264-HD-PL": "\00311X264-HD-PL\003",
        "X264-HD-FR": "\00311X264-HD-FR\003",
        "X264-HD-DE": "\00311X264-HD-DE\003",
        "X264-HD-CZ": "\00311X264-HD-CZ\003",
        "X264-HD-ES": "\00311X264-HD-ES\003",
        "X264-HD-SP": "\00311X264-HD-SP\003",
        "X264-HD-ES": "\00311X264-HD-ES\003",    
        "X264-HD-NORDiC": "\00311X264-HD-NORDiC\003",

        "X264-SD-NL": "\00311X264-SD-NL\003",
        "X264-SD-IT": "\00311X264-SD-IT\003",
        "X264-SD-PL": "\00311X264-SD-PL\003",
        "X264-SD-FR": "\00311X264-SD-FR\003",     
        "X264-SD-DE": "\00311X264-SD-DE\003",
        "X264-SD-CZ": "\00311X264-SD-CZ\003",
        "X264-SD-ES": "\00311X264-SD-ES\003",
#X265
        "X265": "\00311X265\003",
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
# ANiME
        "ANiME": "\00306ANiME\003",
# SPORT
        "SPORTS": "\00314SPORTS\003",
# GAMES
        "GAMES": "\00310GAMES\003",
        "GAMES-0DAY": "\00310GAMES-0DAY\003",
        "DC": "\00310DC\003",
        "WII": "\00310WII\003",
        "PSX": "\00310PSX\003",  
        "PSV": "\00310PSV\003",        
        "PSP": "\00310PSP\003",  
        "PS2": "\00310PSP2\003",              
        "PS3": "\00310PSP3\003",
        "PS4": "\00310PSP4\003",
        "PS5": "\00310PSP5\003",
        "GBA": "\00310GBA\003",
        "GBC": "\00310GBC\003",        
        "NGC": "\00310NGC\003",
        "NDS": "\00310NDS\003",
        "3DS": "\003103DS\003",
        "NSW": "\00310NSW\003",
        "XBOX": "\00310XBOX\003",
        "XBOX360": "\00310XBOX360\003",
        "GAMES-CONSOLE": "\00310GAMES-CONSOLE\003",
        "GAMES-NiNTENDO": "\00310GAMES-NiNTENDO\003",              
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
    
    def __init__(self, irc):
        super().__init__(irc)
        self.target_irc_state = None
        self.session = requests.Session()
        self.db_path = '/home/klapvogn/limnoria/plugins/PreDB/predb.db'
        self.passphrase = os.getenv("SQLITE_PASSPHRASE")
        
        # Create thread pool for background tasks
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        
        # Initialize caches for HTTP responses
        self.nfo_cache = OrderedDict()
        self.sfv_cache = OrderedDict()
        self.srr_cache = OrderedDict()
        self.cache_maxsize = 1000
        
        if not self.passphrase:
            raise ValueError("SQLITE_PASSPHRASE environment variable is not set.")
        
        # Initialize database
        self.initialize_db()

    # ========================
    # DATABASE CONNECTION POOL
    # ========================
    @contextlib.contextmanager
    def db_connection(self):
        """Context manager for database connections with automatic cleanup"""
        conn = sqlcipher.connect(self.db_path)
        try:
            conn.execute(f"PRAGMA key = '{self.passphrase}'")
            conn.execute("PRAGMA cache_size = -200000")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA journal_mode = WAL")
            yield conn
        finally:
            conn.close()

    # =================
    # DATABASE OPERATIONS
    # =================
    def initialize_db(self):
        """Creates database schema and indexes"""
        with self.db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS releases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    releasename TEXT UNIQUE NOT NULL,
                    section TEXT,
                    unixtime INTEGER DEFAULT (strftime('%s', 'now')),
                    files INTEGER,
                    size INTEGER,
                    grp TEXT,
                    genre TEXT,
                    nuked INTEGER,
                    reason TEXT,
                    nukenet TEXT
                )
            ''')
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_releasename ON releases(releasename)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_section ON releases(section)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_grp ON releases(grp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_nuked ON releases(nuked)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_unixtime ON releases(unixtime)')
            # New index for group stats
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_grp_nuked ON releases(grp, nuked)')
            conn.commit()            
    
    # ========================
    # CACHED HTTP REQUESTS
    # ========================
    @lru_cache(maxsize=1000)
    def get_cached_url(self, url):
        """Cached HTTP GET with timeout"""
        try:
            response = self.session.get(url, timeout=5)
            return response
        except Exception:
            return None

    def get_nfo_from_srrdb(self, releasename):
        """Cached NFO lookup with timeout"""
        url = f"https://api.srrdb.com/v1/nfo/{releasename}"
        response = self.get_cached_url(url)
        
        if response and response.status_code == 200:
            data = response.json()
            if data.get('nfolink'):
                nfo_url = data['nfolink'][0]
                shortened = self.shorten_url(nfo_url)
                return f"[ \x033NFO\x03: {shortened} ]"
        return f"[ \x0305NFO\x03 ]"

    def get_sfv_from_srrdb(self, releasename):
        """Cached SFV lookup with timeout"""
        url = f"https://api.srrdb.com/v1/nfo/{releasename}"
        response = self.get_cached_url(url)
        
        if response and response.status_code == 200:
            data = response.json()
            if data.get('nfolink'):
                sfv_url = data['nfolink'][0].replace('.nfo', '.sfv')
                shortened = self.shorten_url(sfv_url)
                return f"[ \x033SFV\x03: {shortened} ]"
        return f"[ \x0305SFV\x03 ]"

    def get_srr_from_srrdb(self, releasename):
        """Cached SRR lookup with timeout"""
        url = f"https://www.srrdb.com/download/srr/{releasename}"
        response = self.get_cached_url(url)
        
        if response and response.status_code == 200:
            shortened = self.shorten_url(url)
            return f"[ \x033SRR\x03: {shortened} ]"
        return f"[ \x0305SRR\x03 ]"

    def get_all_links(self, releasename):
        """Get all links in parallel using cached functions"""
        with ThreadPoolExecutor() as executor:
            nfo_future = executor.submit(self.get_nfo_from_srrdb, releasename)
            sfv_future = executor.submit(self.get_sfv_from_srrdb, releasename)
            srr_future = executor.submit(self.get_srr_from_srrdb, releasename)
            return (
                nfo_future.result(), 
                sfv_future.result(), 
                srr_future.result()
            )

    # ========================
    # URL SHORTENING
    # ========================
    @lru_cache(maxsize=1000)
    def shorten_url(self, long_url):
        """Cached URL shortening with timeout"""
        try:
            tinyurl_api = f"https://tinyurl.com/api-create.php?url={long_url}"
            response = self.session.get(tinyurl_api, timeout=3)
            return response.text if response.status_code == 200 else long_url
        except Exception:
            return long_url
        
    # ========================
    # TIME FORMATTING
    # ========================
    def format_time_ago(self, timestamp):
        """Efficient time difference calculation"""
        now = time.time()
        diff = int(now - timestamp)
        if diff < 60:
            return f"{diff} second{'s' if diff != 1 else ''} ago"
        elif diff < 3600:
            minutes = diff // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif diff < 86400:
            hours = diff // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        else:
            days = diff // 86400
            return f"{days} day{'s' if days != 1 else ''} ago"

    # Pre Search Cache
    @lru_cache(maxsize=100)  # Caches the last 100 queries to improve performance
    def fetch_release(self, release):
        with self.get_connection() as conn:
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
                    WHERE releasename = ?
                    LIMIT 1;
                """, (release,))
            return cursor.fetchone()
    # End

# CHANGE UNIXTIME
    def unixtime(self, irc, msg, args):
        """Handles the `+unixtime` command to update the unixtime for a release."""

        # Security: Check if the user is 'klapvogn'
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

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
            # Connect to the encrypted database using SQLCipher
            with self._get_connection() as conn:
                # Execute the update query
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET unixtime = ? WHERE releasename = ?",
                    (new_unixtime, releasename)
                )

                # Check if any rows were affected
                if cursor.rowcount > 0:
                    conn.commit()
                    irc.reply(f"Updated unixtime for release: {releasename} to {new_unixtime}.")
                else:
                    irc.reply(f"Release {releasename} not found in the database.")
        except sqlcipher.DatabaseError as e:
            self.log.error(f"SQLCipher database error during unixtime change: {e}")
            irc.reply(f"Error during unixtime change: {e}")
        except sqlite3.Error as e:
            self.log.error(f"Error during unixtime change: {e}")
            irc.reply(f"Error during unixtime change: {e}")
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")

# CHANGE SECTION
    def chgsec(self, irc, msg, args):
        """Handles the `!chgsec` command to update the section for a release.

        Usage: !chgsec <releasename> <new_section>
        """

        # Security: Check if the user is 'klapvogn'
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        if len(args) < 2:
            irc.reply("Usage: !chgsec <releasename> <new_section>")
            return

        releasename, new_section = args[0], args[1]

        # Retrieve the passphrase from environment variable
        passphrase = os.getenv("SQLITE_PASSPHRASE")
        if not passphrase:
            irc.reply("Error: Database passphrase is not set.")
            return

        try:
            # Connect to the database using SQLCipher
            with self._get_connection() as conn:
                
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE releases SET section = ? WHERE releasename = ?",
                    (new_section, releasename)
                )
                if cursor.rowcount > 0:
                    conn.commit()
                    irc.reply(f"Updated section for release: {releasename} to {new_section}.")
                else:
                    irc.reply(f"Release {releasename} not found in the database.")
        except sqlcipher.DatabaseError as e:
            self.log.error(f"SQLCipher database error during section change: {e}")
            irc.reply(f"Error during section change: {e}")
        except sqlite3.Error as e:
            self.log.error(f"Error during section change: {e}")
            irc.reply(f"Error during section change: {e}")
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")

    # ========================
    # COMMAND: PRE
    # ========================
    def pre(self, irc, msg, args, release):
        """<release> -- Fetches pre-release data with optimized queries"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                if release == "*":
                    cursor.execute("""
                        SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet 
                        FROM releases 
                        ORDER BY unixtime DESC 
                        LIMIT 1
                    """)
                else:
                    cursor.execute("""
                        SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet 
                        FROM releases 
                        WHERE releasename = ?
                        LIMIT 1
                    """, (release,))
                result = cursor.fetchone()

            if not result:
                irc.reply(f"\x0305Nothing found for\x03: {release}" if release != "*" else "\x0305No releases found.")
                return 

            # Unpack and process result
            releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet = result
            section_formatted = self.section_colors.get(section, section)
            time_ago = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            info_string = f"[ \x033INFO\x03: {size} MB, {files} Files ] " if size and files else ""
            
            # Nuke status mapping
            nuke_status = {
                1: ("\x0305Nuked", "\x0305"),
                2: ("\x033UnNuked", "\x033"),
                3: ("\x035ModNuked", "\x035")
            }
            status, color = nuke_status.get(nuked, ("", ""))
            nuked_details = f"[ {status}: {reason or 'No reason'}{color} => {nukenet or 'Unknown'}{color} ]" if status else ""
            
            # Get links in parallel
            nfo_text, sfv_text, srr_text = self.get_all_links(releasename)
            
            # Build response
            section_and_genre = f"[ {section_formatted} / {genre} ]" if genre and genre.lower() != 'null' else f"[ {section_formatted} ]"
            message = (
                f"\x033[ PRED ]\x03 [ {releasename} ] [ \x033TIME\x03: {time_ago} / {pretime_formatted} ] "
                f"in {section_and_genre} {info_string}{nuked_details}"
                f"{nfo_text}{sfv_text}{srr_text}"
            )
            irc.reply(message)

        except Exception as e:
            self.log.error(f"Error in pre: {e}")
            irc.reply(f"Error retrieving pre data: {str(e)}")
    pre = commands.wrap(pre, ['text'])

    def dupe(self, irc, msg, args, release):
        """<release> -- Fetches pre-release data based on a search term for duplicates"""

        # Clean and format the release input
        sea1 = release.replace("%", "*").strip()
        sea1 = sea1.lower()  # Normalize to lowercase for case-insensitive matching

        try:
            # Use the connection via the context manager
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Execute the query for duplicates based on the release name
                cursor.execute("""
                    SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet
                    FROM releases
                    WHERE releasename LIKE ?
                    ORDER BY unixtime DESC
                    LIMIT 10
                """, (f"{sea1}%",))
                results = cursor.fetchall()

            # If no results are found
            if not results:
                irc.reply(f"\x0305Nothing found for\x03: {release}")
                return

            # Notify the user about sending results
            irc.reply(f"PM'ing last 10 results to {msg.nick}")

            # List to accumulate all messages
            messages = []
            current_time = int(time.time())  # Define current_time before the loop

            # Loop over all the results and build messages
            for result in results:
                # Unpack the result, skipping the first column (id)
                releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet = result

                # Lookup the section color
                section_formatted = self.section_colors.get(section, section)  # Default to section name if not found 

                # Convert unixtime to a readable format
                time_ago = self.format_time_ago(unixtime)
                pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")

                # Build the nuked details
                nuked_details = {
                    1: f"[ \x0305Nuked: {reason or 'No reason'}\x03 => \x0305{nukenet or 'Unknown'}\x03 ]",
                    2: f"[ \x033UnNuked: {reason or 'No reason'}\x03 => \x033{nukenet or 'Unknown'}\x03 ]",
                    3: f"[ \x035ModNuked: {reason or 'No reason'}\x03 => \x035{nukenet or 'Unknown'}\x03 ]"
                }.get(nuked, "")

                # Include genre only if it exists and is not NULL
                section_and_genre = f"[ {section_formatted} / {genre} ]" if genre and genre.lower() != 'null' else f"[ {section_formatted} ]"

                info_string = f"[ \x033INFO\x03: {size} MB, {files} Files ] " if size and files else ""

                # Get NFO text using the cache method
                nfo_text = self.get_nfo_cached(releasename)

                # Build the message to send for each result
                message = f"\x033[ PRED ]\x03 [ {releasename} ] [ \x033TIME\x03: {time_ago} / {pretime_formatted} ] in {section_and_genre} {info_string}{nuked_details}{nfo_text}"

                # Add the message to the list of messages
                messages.append(message)

            # Send all messages to the user
            for msg in messages:
                irc.reply(msg, private=True)

        except Exception as e:
            self.log.error(f"Error during dupe search: {e}")
            irc.reply(f"Error during dupe search: {e}")

    dupe = commands.wrap(dupe, ['text'])

    # ========================
    # COMMAND: GROUP
    # ========================
    def group(self, irc, msg, args, groupname):
        """+group <groupname> - Optimized group statistics"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS total_releases,
                        SUM(nuked = 1) AS nukes,
                        SUM(nuked = 2) AS unnukes,
                        MIN(unixtime) AS first_pre_time,
                        MAX(unixtime) AS last_pre_time,
                        (SELECT releasename FROM releases WHERE grp = ? ORDER BY unixtime ASC LIMIT 1),
                        (SELECT releasename FROM releases WHERE grp = ? ORDER BY unixtime DESC LIMIT 1)
                    FROM releases
                    WHERE grp = ?
                """, (groupname, groupname, groupname))

                result = cursor.fetchone()
                if not result or not result[0]:
                    irc.reply(f"\x0305Nothing found for\x03: {groupname}")
                    return

                total, nukes, unnukes, first_time, last_time, first_release, last_release = result
                first_time_fmt = datetime.utcfromtimestamp(first_time).strftime('%Y-%m-%d %H:%M:%S') if first_time else "N/A"
                last_time_fmt = datetime.utcfromtimestamp(last_time).strftime('%Y-%m-%d %H:%M:%S') if last_time else "N/A"

                irc.reply(f"\x033[ GROUP ]\x03 [ {groupname} ] [ \x033Releases\x03: {total} ] "
                          f"[ \x0305NUKES\03: {nukes} ] [ \x033UNNUKES\03: {unnukes} ]")
                
                if first_release:
                    irc.reply(f"\x037[ FIRST RELEASE\x03 ] {first_release} [ Time: {first_time_fmt} ]")
                if last_release:
                    irc.reply(f"\x033[ LAST RELEASE\x03 ] {last_release} [ Time: {last_time_fmt} ]")

        except Exception as e:
            self.log.error(f"Error in group: {e}")
            irc.reply(f"Error retrieving group data: {str(e)}")
    group = commands.wrap(group, ['text'])

# LASTNUKE
    @wrap([optional('text')])
    def lastnuke(self, irc, msg, args, groupname=None):
        """[<groupname>] - Fetch the most recent nuked release. Optionally filter by group name."""
        
        try:
            # Use the context manager to handle connection automatically
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Base query to fetch the most recent nuked release
                query = """
                    SELECT 
                        releasename, 
                        unixtime,
                        section,
                        reason,
                        nukenet
                    FROM releases
                    WHERE nuked = 1
                """
                params = []

                # If a group name is provided, add it to the query
                if groupname:
                    query += " AND grp = ?"
                    params.append(groupname)

                # Order by most recent nuked release
                query += " ORDER BY unixtime DESC LIMIT 1"

                # Execute the query
                cursor.execute(query, params)
                result = cursor.fetchone()

                if not result:
                    if groupname:
                        irc.reply(f"\x0305No nuked releases found for group\x03: {groupname}")
                    else:
                        irc.reply("\x0305No nuked releases found.")
                    return

                releasename, unixtime, section, reason, nukenet = result
                # Lookup the section color
                section_formatted = self.section_colors.get(section, section)  # Default to section name if not found
                # Optimized time calculations
                time_ago = self.format_time_ago(unixtime)
                pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
                irc.reply(
                    f"[ \x0305NUKED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_formatted} ] "
                    f"in [ {section_formatted} ] [ \x0305{reason or 'Unknown reason'}\x03 => \x0305{nukenet or 'Unknown network'}\x03 ]"
                )
        except sqlcipher.DatabaseError as e:
            self.log.error(f"SQLCipher database error during lastnuke: {e}")
            irc.reply(f"Error lastnuke search: {e}")
        except sqlite3.Error as e:
            self.log.error(f"Error lastnuke search: {e}")
            irc.reply(f"Error lastnuke search: {e}")
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")

# LASTUNNUKE
    @wrap([optional('text')])
    def lastunnuke(self, irc, msg, args, groupname=None):
        """[<groupname>] - Fetch the most recent unnuked release. Optionally filter by group name."""

        try:
            # Connect to the SQLCipher database
            with self._get_connection() as conn:
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
                WHERE nuked = 2
            """
            params = []

            # If a group name is provided, add it to the query
            if groupname:
                query += " AND grp = ?"
                params.append(groupname)

            # Order by most recent nuked release
            query += " ORDER BY unixtime DESC LIMIT 1"

            # Execute the query
            cursor.execute(query, params)
            result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"\x0305No unnuked releases found for group\x03: {groupname}")
                else:
                    irc.reply("\x0305No unnuked releases found.")
                return

            releasename, unixtime, section, reason, nukenet = result
            # Lookup the section color
            section_formatted = self.section_colors.get(section, section)  # Default to section name if not found
            # Optimized time calculations
            time_ago = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            irc.reply(
                f"[ \x0303UNNUKED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_formatted} ] "
                f"in [ {section_formatted} ] [ \x0303{reason or 'Unknown reason'}\x03 => \x0303{nukenet or 'Unknown network'}\x03 ]"
            )
        except sqlcipher.DatabaseError as e:
            self.log.error(f"SQLCipher database error during lastunnuke: {e}")
            irc.reply(f"Error lastunnuke search: {e}")
        except sqlite3.Error as e:
            self.log.error(f"Error lastunnuke search: {e}")
            irc.reply(f"Error lastunnuke search: {e}")
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")
# SECTION
    def section(self, irc, msg, args, section=None):
        """[<section>] - Fetch the most recent nuked releases, filtered by section. Limit to 10 results."""
        
        try:
            # Connect to the SQLCipher-encrypted SQLite3 database
            # Connect to the SQLCipher database
            with self._get_connection() as conn:
                cursor = conn.cursor()

            # Base query to fetch nuked releases filtered by section
            query = """
                SELECT 
                    releasename, 
                    unixtime,
                    section,
                    reason,
                    nukenet,
                    size,
                    files
                FROM releases
                WHERE nuked = 1
            """
            params = []

            # If a section is provided, add it to the query
            if section:
                query += " AND section = ?"
                params.append(section)

            # Order by most recent nuked releases, limit to 10
            query += " ORDER BY unixtime DESC LIMIT 10"

            # Execute the query
            cursor.execute(query, params)
            results = cursor.fetchall()

            if not results:
                irc.reply(f"\x0305Nothing found for\x03: {section}")
                return
            
            # Notify the user about sending results
            irc.reply(f"PM'ing last 10 results to {msg.nick}")

            # List to accumulate all messages
            messages = []   

            # Process the results
            for result in results:
                releasename, unixtime, section, reason, nukenet, size, files = result

                # Lookup the section color
                section_formatted = self.section_colors.get(section, section)  # Default to section name if not found

                # Build the info string
                info_string = f"[ INFO: {size} MB, {files} Files ] " if size and files else ""                
                    
                # Optimized time calculations
                time_ago = self.format_time_ago(unixtime)
                pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")

                # Send the formatted message as a private message to the user
                message = f"[ \x033PRED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_formatted} ] in [ {section_formatted} ] {info_string}"
                # Add the message to the list of messages
                messages.append(message)

            # Send all messages to the user
            for msg in messages:
                irc.reply(msg, private=True)                

        except sqlcipher.DatabaseError as e:
            self.log.error(f"SQLCipher database error during section: {e}")
            irc.reply(f"Error section search: {e}")
        except sqlite3.Error as e:
            self.log.error(f"Error section search: {e}")
            irc.reply(f"Error section search: {e}")
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")
    section = commands.wrap(section, ['text'])

    # ========================
    # BACKGROUND TASK HANDLING
    # ========================
    def handle_addpre(self, irc, msg, args):
        """Threadpool-based addpre handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn"]:
            return
            
        if len(args) < 2:
            irc.reply("Usage: !addpre <releasename> <section>")
            return
        
        releasename, section = args[0], args[1]
        group = releasename.split('-')[-1] if '-' in releasename else None
        
        # Submit to thread pool
        self.thread_pool.submit(self._addpre_thread, irc, releasename, section, group)

    def _addpre_thread(self, irc, releasename, section, group):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                # Use INSERT OR IGNORE to handle duplicates
                cursor.execute(
                    "INSERT OR IGNORE INTO releases (releasename, section, grp) VALUES (?, ?, ?)",
                    (releasename, section, group),
                )
                conn.commit()
                if cursor.rowcount > 0:
                    self.announce_pre(irc, releasename, section)
                else:
                    irc.reply(f"Release '{releasename}' already exists")
        except Exception as e:
            self.log.error(f"Addpre error: {e}")          

    # ========================
    # ADDNUKE (UPDATED)
    # ========================
    def handle_addnuke(self, irc, msg, args):
        """Threadpool-based nuke handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn"]:
            return
            
        if len(args) < 3:
            irc.reply("Usage: !nuke <releasename> <reason> <nukenet>")
            return

        releasename = args[0]
        reason = ' '.join(args[1:-1])
        nukenet = args[-1]
        
        # Submit to thread pool
        self.thread_pool.submit(self._nuke_thread, irc, releasename, reason, nukenet)

    def _nuke_thread(self, irc, releasename, reason, nukenet):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Check if already nuked
                cursor.execute("SELECT nuked FROM releases WHERE releasename = ?", (releasename,))
                result = cursor.fetchone()

                if result and result[0] == 1:
                    irc.reply(f"Release {releasename} is already nuked.")
                    return

                # Perform the update
                cursor.execute(
                    "UPDATE releases SET nuked = ?, reason = ?, nukenet = ? WHERE releasename = ?",
                    (1, reason, nukenet, releasename),
                )
                conn.commit()

                if cursor.rowcount > 0:
                    self.announce_nuke_status(irc, releasename, reason, nukenet, 1)
                else:
                    irc.reply(f"Release {releasename} not found in database")

        except Exception as e:
            self.log.error(f"Nuke error: {e}")
            irc.reply(f"Error processing nuke: {str(e)}")

    # ========================
    # ADDUNNUKE (UPDATED)
    # ========================
    def handle_addunnuke(self, irc, msg, args):
        """Threadpool-based unnuke handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn"]:
            return
            
        if len(args) < 3:
            irc.reply("Usage: !unnuke <releasename> <reason> <nukenet>")
            return

        releasename = args[0]
        reason = ' '.join(args[1:-1])
        nukenet = args[-1]
        
        # Submit to thread pool
        self.thread_pool.submit(self._unnuke_thread, irc, releasename, reason, nukenet)

    def _unnuke_thread(self, irc, releasename, reason, nukenet):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Check if release exists and status
                cursor.execute("SELECT nuked FROM releases WHERE releasename = ?", (releasename,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Release {releasename} not found")
                    return
                    
                if result[0] == 2:
                    irc.reply(f"Release {releasename} is already unnuked.")
                    return

                # Perform the update
                cursor.execute(
                    "UPDATE releases SET nuked = ?, reason = ?, nukenet = ? WHERE releasename = ?",
                    (2, reason, nukenet, releasename),
                )
                conn.commit()

                if cursor.rowcount > 0:
                    self.announce_nuke_status(irc, releasename, reason, nukenet, 2)
                else:
                    irc.reply(f"Failed to unnuke {releasename}")

        except Exception as e:
            self.log.error(f"Unnuke error: {e}")
            irc.reply(f"Error processing unnuke: {str(e)}")

    # ========================
    # MODNUKE (UPDATED)
    # ========================
    def handle_addmodnuke(self, irc, msg, args):
        """Threadpool-based modnuke handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn"]:
            return
            
        if len(args) < 3:
            irc.reply("Usage: !modnuke <releasename> <reason> <nukenet>")
            return

        releasename = args[0]
        reason = ' '.join(args[1:-1])
        nukenet = args[-1]
        
        # Submit to thread pool
        self.thread_pool.submit(self._modnuke_thread, irc, releasename, reason, nukenet)

    def _modnuke_thread(self, irc, releasename, reason, nukenet):
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Check if release exists and status
                cursor.execute("SELECT nuked FROM releases WHERE releasename = ?", (releasename,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Release {releasename} not found")
                    return
                    
                if result[0] == 3:
                    irc.reply(f"Release {releasename} is already modnuked.")
                    return

                # Perform the update
                cursor.execute(
                    "UPDATE releases SET nuked = ?, reason = ?, nukenet = ? WHERE releasename = ?",
                    (3, reason, nukenet, releasename),
                )
                conn.commit()

                if cursor.rowcount > 0:
                    self.announce_nuke_status(irc, releasename, reason, nukenet, 3)
                else:
                    irc.reply(f"Failed to modnuke {releasename}")

        except Exception as e:
            self.log.error(f"Modnuke error: {e}")
            irc.reply(f"Error processing modnuke: {str(e)}")

    # ========================
    # ADDINFO (UPDATED)
    # ========================
    def handle_addinfo(self, irc, msg, args):
        """Threadpool-based info handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn"]:
            return
            
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
                    "UPDATE releases SET files = ?, size = ? WHERE releasename = ?",
                    (files, size, releasename),
                )
                conn.commit()

        except Exception as e:
            self.log.error(f"Addinfo error: {e}")


    def announce_pre(self, irc, releasename, section):
        """Announce new release to the #omgwtfnzbs.pre channel on another network."""
        #self.log.info(f"Announcing release: {section} {releasename}")
        # Define the target channel
        target_channel = "#omgwtfnzbs.pre"

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
    # UNIFIED ANNOUNCEMENTS
    # ========================
    def announce_nuke_status(self, irc, releasename, reason, nukenet, nuke_type):
        """Unified nuke announcement handler"""
        nuke_types = {
            1: ("NUKE", "05"),
            2: ("UNNUKE", "03"),
            3: ("MODNUKE", "05")
        }
        name, color = nuke_types.get(nuke_type, ("UNKNOWN", "05"))
        announcement = f"[ \x03{color}{name}\x03 ] {releasename} [ \x03{color}{reason}\x03 ] => \x03{color}{nukenet}\x03"
        
        if irc_state := self._get_target_irc_state():
            irc_state.queueMsg(ircmsgs.privmsg("#omgwtfnzbs.pre", announcement))

    # Update nuke handlers to use this unified method:
    # - In _nuke_thread: self.announce_nuke_status(irc, releasename, reason, nukenet, 1)
    # - In _unnuke_thread: self.announce_nuke_status(irc, releasename, reason, nukenet, 2)
    # - In _modnuke_thread: self.announce_nuke_status(irc, releasename, reason, nukenet, 3)

    def doPrivmsg(self, irc, msg):
        """Intercepts private messages to parse `!addpre` and `!info` commands."""
        text = msg.args[1]
        if text.startswith("!addpre"):
            args = text.split()[1:]  # Extract arguments after the command
            self.handle_addpre(irc, msg, args)
        elif text.startswith("!info"):
            args = text.split()[1:]  # Extract arguments after the command
            self.handle_addinfo(irc, msg, args)
        elif text.startswith("!nuke"):
            args = text.split()[1:]  # Extract arguments after the command
            self.handle_addnuke(irc, msg, args)   
        elif text.startswith("!modnuke"):
            args = text.split()[1:]  # Extract arguments after the command
            self.handle_addmodnuke(irc, msg, args)             
        elif text.startswith("!unnuke"):
            args = text.split()[1:]  # Extract arguments after the command
            self.handle_addunnuke(irc, msg, args)    

    # ========================
    # DATABASE STATISTICS
    # ========================
    @lru_cache(maxsize=1)
    def _get_db_stats_cached(self, _cache_key):
        """Cached database statistics with optimized query"""
        try:
            start_of_today = datetime.combine(date.today(), datetime_time.min).timestamp()
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        COUNT(*) AS total,
                        SUM(CASE WHEN unixtime >= ? THEN 1 ELSE 0 END) AS today,
                        SUM(nuked = 1) AS nukes,
                        SUM(nuked = 2) AS unnukes,
                        SUM(nuked = 3) AS modnukes,
                        (SELECT releasename FROM releases ORDER BY unixtime DESC LIMIT 1)
                    FROM releases
                """, (int(start_of_today),))
                return cursor.fetchone()
        except Exception:
            return None

    def db(self, irc, msg, args):
        """Efficient stats with time-based cache invalidation"""
        cache_key = int(time.time() // 60)  # Cache for 60 seconds
        stats = self._get_db_stats_cached(cache_key)
        if stats:
            total, today, nuked, unnuked, modnuked, last_pre = stats
            irc.reply(
                f"[ PRE DATABASE ] [ \x033RELEASES\x03: {self.human_readable_number(total)} ] "
                f"[ \x033TODAY\x03: {self.human_readable_number(today)} ] [ \x0305NUKES\x03: {self.human_readable_number(nuked)} ] "
                f"[ \x033UNNUKES\x03: {self.human_readable_number(unnuked)} ] [ \x034MODNUKES\x03: {self.human_readable_number(modnuked)} ] "
                f"[ \x0306Last Pre\x03: {last_pre or 'None'} ]"
            )
        else:
            irc.reply("Error retrieving database stats")
            return None
        
    # ========================
    # HELPER FUNCTIONS
    # ========================
    def _get_target_irc_state(self):
        """Cached IRC state lookup"""
        if self.target_irc_state and self.target_irc_state in world.ircs:
            return self.target_irc_state
            
        for irc_state in world.ircs:
            if "omg" in irc_state.network:
                self.target_irc_state = irc_state
                return irc_state
        return None
    
    def human_readable_number(self, n):
        """Convert large numbers into a human-readable format with commas (e.g., 12,345,678)."""
        return f"{n:,}"

    def prehelp(self, irc, msg, args):
        """Sends help information about the Kudos plugin in a private message."""
        help_messages = [
            "\x02\x1f:: PREHELP ::\x1f\x02",
            " ",  # This adds an empty line
            "+pre <releasename>: To search for one specific result.",
            "+dupe <part of the releasename>: Last 10 in private",
            "+group <group name>: Shows group stats of the specified group, in private",
            "+lastnuke <group>: Shows last nuked release in private (Groupname is optional).",
            "+lastunnuke <group>: Shows last unnuked release in private (Groupname is optional).",
            "+section <section> : Shows last 10 releases in the selected section",
            "+db : Shows statistics of the DataBase.",
        ]
        for message in help_messages:
            irc.reply(message, private=True)


Class = PreDB
