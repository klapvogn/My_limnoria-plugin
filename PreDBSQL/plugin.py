import mysql.connector
from mysql.connector import Error
import time
import queue
import threading
import json
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
from threading import Thread
from collections import OrderedDict
from pathlib import Path
#import psutil
#import re
#import traceback
#from difflib import SequenceMatcher

# Load .env from the plugin directory
plugin_dir = Path(__file__).parent
env_path = plugin_dir / '.env'
load_dotenv(dotenv_path=env_path)

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
    
    def __init__(self, irc):
        super().__init__(irc)
        self.log = world.log
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

    
        
        # Create thread pool with better error handling
        self.thread_pool = ThreadPoolExecutor(
            max_workers=3,  # Reduced from 5 to prevent resource exhaustion
            thread_name_prefix='predb_worker'
        )

        # Track active tasks
        self._active_tasks = 0
        self._max_active_tasks = 0        
        
        # Initialize caches for HTTP responses
        self.cache_maxsize = 50
        self.nfo_cache = OrderedDict()
        self.sfv_cache = OrderedDict()
        self.srr_cache = OrderedDict()
               
    # ========================
    # DATABASE CONNECTION POOL
    # ========================
    # Optional: Add connection pool
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="predb_pool",
            pool_size=5,
            **self.db_config
        )

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
            "!delpre": self.handle_adddelpre,
            "!undelpre": self.handle_addundelpre,
            "!modnuke": self.handle_addmodnuke,
            "!unnuke": self.handle_addunnuke,
        }

    def _process_command(self, text):
        """Process commands efficiently"""
        self.log.info(f"Processing command: {text}")
        
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
        """Context manager for MySQL database connections with automatic cleanup"""
        conn = None
        try:
            self.log.debug(f"Attempting to connect with config: host={self.db_config['host']}, db={self.db_config['database']}, user={self.db_config['user']}")
            conn = mysql.connector.connect(**self.db_config)
            self.log.debug("MySQL connection established successfully")
            yield conn
        except Error as e:
            self.log.error(f"MySQL connection error: {e}")
            if conn:
                conn.close()
            raise e
        finally:
            if conn and conn.is_connected():
                conn.close()
                self.log.debug("Database connection closed")

    # ========================
    # CACHED HTTP REQUESTS
    # ========================
    @lru_cache(maxsize=100)
    def get_cached_content(self, url):
        """Cached HTTP GET with proper size enforcement"""
        if url in self.nfo_cache:
            # Move to end (most recently used)
            content = self.nfo_cache.pop(url)
            self.nfo_cache[url] = content
            return content
        
        try:
            response = self.session.get(url, timeout=5)
            content = response.text if response.status_code == 200 else None
            
            # Enforce cache size limit
            if len(self.nfo_cache) >= self.cache_maxsize:
                self.nfo_cache.popitem(last=False)  # Remove oldest
            
            self.nfo_cache[url] = content
            return content
        except Exception:
            return None

    def get_nfo_from_srrdb(self, releasename):
        """Cached NFO lookup with timeout"""
        url = f"https://api.srrdb.com/v1/nfo/{releasename}"
        content = self.get_cached_content(url)
        
        if content:
            try:
                # Use json.loads instead of eval for safety and proper JSON parsing
                data = json.loads(content)
                if data.get('nfolink'):
                    nfo_url = data['nfolink'][0]
                    shortened = self.shorten_url(nfo_url)
                    return f"[ \x033NFO\x03: {shortened} ] "
            except (json.JSONDecodeError, KeyError, IndexError):
                # Handle JSON parsing errors or missing keys
                pass
        return f"[ \x0305NFO\x03 ]"
    
    def get_sfv_from_srrdb(self, releasename):
        """Cached SFV lookup with timeout"""
        url = f"https://api.srrdb.com/v1/nfo/{releasename}"
        content = self.get_cached_content(url)
        
        if content:
            try:
                data = json.loads(content)
                if data.get('nfolink'):
                    sfv_url = data['nfolink'][0].replace('.nfo', '.sfv')
                    shortened = self.shorten_url(sfv_url)
                    return f"[ \x033SFV\x03: {shortened} ] "
            except (json.JSONDecodeError, KeyError, IndexError):
                pass
        return f"[ \x0305SFV\x03 ]"

    def get_srr_from_srrdb(self, releasename):
        """Cached SRR lookup with timeout"""
        url = f"https://www.srrdb.com/download/srr/{releasename}"
        content = self.get_cached_content(url)
        
        # For SRR, we just need to check if the URL exists (returns 200)
        if content is not None:  # content will be None if request failed or not 200
            shortened = self.shorten_url(url)
            return f"[ \x033SRR\x03: {shortened} ] "
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
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")

    # ==============
    # CHANGE SECTION
    # ==============  
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
        # Security: Check if the user is 'klapvogn'
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

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
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            irc.reply(f"Unexpected error: {e}")

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
            
            # Get links in parallel
            nfo_text, sfv_text, srr_text = self.get_all_links(releasename)
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

        except Exception as e:
            self.log.error(f"Error in pre: {e}")
            irc.reply(f"Error retrieving pre data: {str(e)}")
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

        try:
            # Use the connection via the context manager
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Execute the query for duplicates based on the release name
                cursor.execute("""
                    SELECT releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet
                    FROM releases
                    WHERE releasename LIKE %s
                    ORDER BY unixtime DESC
                    LIMIT 10
                """, (f"{sea1}%",))
                results = cursor.fetchall()

            # If no results are found
            if not results:
                irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
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

                # Get links in parallel
                nfo_text, sfv_text, srr_text = self.get_all_links(releasename)
                # Info and section        
                info_string = f"[ \x033INFO\x03: {size} MB, {files} Files ] " if size and files else ""
                section_and_genre = f"[ {section_formatted} / {genre} ]" if genre and genre.lower() != 'null' else f"[ {section_formatted} ]"

                # Nuke status mapping
                nuke_status = {
                    '1': ("\x0304Nuked\x03", "\x0304"),
                    '2': ("\x0303UnNuked\x03", "\x0303"),
                    '3': ("\x0305ModNuked\x03", "\x0305"),
                    '4': ("\x0305DelPred\x03", "\x0305"),
                    '5': ("\x0303UnDelPred\x03", "\x0303")
                }
                status, color = nuke_status.get(nuked, ("", ""))
                nuked_details = f"[ {status}: {color}{reason or 'No reason'}\x03 => {color}{nukenet or 'Unknown'}\x03 ]" if status else ""

                # Get links in parallel
                #nfo_text, sfv_text, srr_text = self.get_all_links(releasename)

                # Build the message to send for each result
                message = f"\x033[ PRED ]\x03 [ {releasename} ] [ \x033TIME\x03: {time_ago} / {pretime_formatted} ] in {section_and_genre} {info_string}{nuked_details}{nfo_text}{sfv_text}{srr_text}"

                # Add the message to the list of messages
                messages.append(message)

            # Send all messages to the user
            for msg in messages:
                irc.reply(msg, private=True)

        except Exception as e:
            self.log.error(f"Error during dupe search: {e}")
            irc.reply(f"Error during dupe search: {e}")

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

        except Exception as e:
            self.log.error(f"Error in group: {e}")
            irc.reply(f"Error retrieving group data: {str(e)}")
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
        except Exception as e:
            self.log.error(f"Error in lastnuke: {e}")
            irc.reply(f"Error retrieving last nuke: {str(e)}")

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
            
        except Exception as e:
            self.log.error(f"Error in lastunnuke: {e}")
            irc.reply(f"Error retrieving unnuke data: {str(e)}")

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
            
        except Exception as e:
            self.log.error(f"Error in lastmodnuke: {e}")
            irc.reply(f"Error retrieving unnuke data: {str(e)}")            

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
            
        except Exception as e:
            self.log.error(f"Error in lastmodnuke: {e}")
            irc.reply(f"Error retrieving unnuke data: {str(e)}") 

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
            
        except Exception as e:
            self.log.error(f"Error in lastmodnuke: {e}")
            irc.reply(f"Error retrieving unnuke data: {str(e)}") 

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

        except Exception as e:
            self.log.error(f"Error in section: {e}")
            irc.reply(f"Error retrieving section data: {str(e)}")
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

    def processpendingurls(self, irc, msg, args):
            """Manually process all pending URLs and match with existing releases"""
            
            # Security check
            if msg.nick != 'klapvogn':
                irc.reply("Error: You do not have permission to use this command.")
                return
            
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
                    
            except Exception as e:
                self.log.error(f"Error in processpendingurls: {e}")
                irc.reply(f"Error processing pending URLs: {str(e)}")
        
    processpendingurls = commands.wrap(processpendingurls, [])


    # Optional: Version with detailed output
    def processpendingurlsverbose(self, irc, msg, args):
        """Manually process all pending URLs with detailed output"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return
        
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
                
        except Exception as e:
            self.log.error(f"Error in processpendingurlsverbose: {e}")
            irc.reply(f"Error processing pending URLs: {str(e)}")
    
    processpendingurlsverbose = commands.wrap(processpendingurlsverbose, [])


    # Optional: Process a single pending URL by releasename
    def processpendingurl(self, irc, msg, args, releasename):
        """<releasename> - Process a specific pending URL"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return
        
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
                    
        except Exception as e:
            self.log.error(f"Error in processpendingurl: {e}")
            irc.reply(f"Error processing pending URL: {str(e)}")
    
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
            self.log.debug(f"Starting _addpre_thread for: {releasename}")
            
            with self.db_connection() as conn:
                self.log.debug("Database connection successful")
                
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT IGNORE INTO releases (releasename, section, grp) VALUES (%s, %s, %s)",
                    (releasename, section, group),
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    self.log.debug("New release, announcing...")
                    
                    # Process pending URL immediately in same transaction
                    self._check_and_process_pending_url_sync(conn, releasename)
                    
                    self.announce_pre(irc, releasename, section)
                else:
                    self.log.debug("Release already exists")
                    irc.reply(f"Release '{releasename}' already exists")
                    
        except Exception as e:
            self.log.error(f"Addpre error: {repr(e)}")

    # ========
    # ADDGENRE
    # ========
    def handle_addgenre(self, irc, msg, args):
        """Threadpool-based genre handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn", "Bette"]:
            return
        
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
                
                # Atomic update: only update if genre is NULL or empty
                cursor.execute(
                    """
                    UPDATE releases 
                    SET genre = %s 
                    WHERE releasename = %s 
                    AND (genre IS NULL OR genre = '' OR TRIM(genre) = '')
                    """,
                    (genre, releasename),
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    self.log.info(f"Updated genre for {releasename} to {genre}")
                    irc.reply(f"Genre updated: {releasename} → {genre}")
                else:
                    # Check why it failed - release doesn't exist or genre already set
                    cursor.execute(
                        "SELECT releasename, genre FROM releases WHERE releasename = %s",
                        (releasename,)
                    )
                    existing = cursor.fetchone()
                    
                    if not existing:
                        self.log.warning(f"No release found with name: {releasename}")
                        # Uncomment if you want to notify about missing releases
                        # irc.reply(f"Release not found: {releasename}")
                    else:
                        current_genre = existing[1]
                        self.log.info(f"Genre already exists for {releasename}: {current_genre}")
                        irc.reply(f"Genre already set: {releasename} → {current_genre}")
                        
        except Exception as e:
            self.log.error(f"Addgenre error: {e}", exc_info=True)
            irc.reply(f"Error updating genre: {str(e)}")       

    # =======
    # ADDURL
    # =======
    def handle_addurl(self, irc, msg, args):
        """Threadpool-based URL handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn", "Bette"]:
            return
        
        if len(args) < 2:
            irc.reply("Usage: !addurl <releasename> <url>")
            return
        
        releasename = args[0]
        url = args[1]
        
        # Log what we're processing
        self.log.info(f"Processing !addurl command: release='{releasename}', url='{url}'")
        
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
                        irc.reply(f"Release {releasename} already queued - updated URL and reset retry counter")
                        return
                    
                    # Not queued yet - add to queue
                    self.url_stats['queued'] += 1

                    # Create pending entry
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
                        irc.reply(f"Release {releasename} not found - URL queued for later processing")
                    else:
                        irc.reply(f"Error queuing URL for {releasename}")
                    return
                
                # Release exists - update it directly
                cursor.execute(
                    "UPDATE releases SET url = %s WHERE releasename = %s",
                    (url, releasename),
                )
                conn.commit()
                
                if cursor.rowcount > 0:
                    self.log.info(f"Updated URL for {releasename} to {url}")

                    # Track immediate success
                    self.url_stats['immediate_success'] += 1                    
                    
                    # Clean up from pending cache/db if it was there
                    self._remove_from_pending_db(releasename)
                        
                    irc.reply(f"URL updated: {releasename} → {url}")
                else:
                    # Check if URL is already the same
                    cursor.execute(
                        "SELECT url FROM releases WHERE releasename = %s",
                        (releasename,)
                    )
                    current_url = cursor.fetchone()
                    if current_url and current_url[0] == url:
                        irc.reply(f"URL already set: {releasename} → {url}")
                    else:
                        self.log.warning(f"URL Update failed for: {releasename}")
                        irc.reply(f"URL already set: {releasename}")
                        
        except Exception as e:
            self.log.error(f"Addurl error: {e}", exc_info=True)
            irc.reply(f"Error updating URL: {str(e)}")

    # =======
    # ADDNUKE
    # =======
    def handle_addnuke(self, irc, msg, args):
        """Threadpool-based nuke handler"""
        try:
            if msg.nick not in ["CTW_PRE", "klapvogn"]:
                return
                
            if len(args) < 3:
                irc.reply("Usage: !nuke <releasename> <reason> <nukenet>")
                return

            releasename = args[0]
            reason = ' '.join(args[1:-1])
            nukenet = args[-1]
            
            self.log.debug(f"Parsed - releasename: {releasename}, reason: {reason}, nukenet: {nukenet}")
            
            # Submit to thread pool
            self.thread_pool.submit(self._nuke_thread, irc, releasename, reason, nukenet)
            
        except Exception as e:
            self.log.error(f"Error in handle_addnuke: {e}")
            irc.reply(f"Error processing nuke command: {e}")

    # ==========================================================
    # Add nukes that is not in database to an new_nukes database
    # ==========================================================
    def _nuke_thread(self, irc, releasename, reason, nukenet):
            """Enhanced nuke handler - adds to new_nukes if not in main database"""
            try:
                with self.db_connection() as conn:
                    cursor = conn.cursor()

                    # Check if release exists in main releases table
                    cursor.execute("SELECT nuked FROM releases WHERE releasename = %s", (releasename,))
                    result = cursor.fetchone()

                    if result:
                        # Release exists - update it
                        if result[0] == 1:
                            irc.reply(f"Release {releasename} is already nuked.")
                            return

                        cursor.execute(
                            "UPDATE releases SET nuked = %s, reason = %s, nukenet = %s WHERE releasename = %s",
                            (1, reason, nukenet, releasename),
                        )
                        conn.commit()

                        if cursor.rowcount > 0:
                            self.announce_nuke_status(irc, releasename, reason, nukenet, 1)
                    else:
                        # Release not found - add to new_nukes instead
                        group = releasename.split('-')[-1] if '-' in releasename else None
                        
                        try:
                            cursor.execute(
                                "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (%s, %s, %s, %s, 1)",
                                (releasename, reason, nukenet, group),
                            )
                            conn.commit()
                            irc.reply(f"Release {releasename} not in database - added to pending nukes. Use +newnukes to review.")
                        except Exception as e:
                            irc.reply(f"Release {releasename} already in pending nukes")
                            self.log.error(f"Error adding to new_nukes: {e}")

            except Exception as e:
                self.log.error(f"Nuke error: {e}")
                irc.reply(f"Error processing nuke: {str(e)}")

    # =======
    # DELPRE
    # =======
    def handle_adddelpre(self, irc, msg, args):
        """Threadpool-based delpre handler"""
        try:
            if msg.nick not in ["CTW_PRE", "klapvogn"]:
                return
                
            if len(args) < 3:
                irc.reply("Usage: !delpre <releasename> <reason> <nukenet>")
                return

            releasename = args[0]
            reason = ' '.join(args[1:-1])
            nukenet = args[-1]
            
            self.log.debug(f"Parsed - releasename: {releasename}, reason: {reason}, nukenet: {nukenet}")
            
            # Submit to thread pool
            self.thread_pool.submit(self._delpre_thread, irc, releasename, reason, nukenet)
            
        except Exception as e:
            self.log.error(f"Error in handle_adddelpre: {e}")
            irc.reply(f"Error processing delpre command: {e}")

    # ==========================================================
    # Add nukes that is not in database to an new_nukes database
    # ==========================================================
    def _delpre_thread(self, irc, releasename, reason, nukenet):
            """Enhanced nuke handler - adds to new_nukes if not in main database"""
            try:
                with self.db_connection() as conn:
                    cursor = conn.cursor()

                    # Check if release exists in main releases table
                    cursor.execute("SELECT nuked FROM releases WHERE releasename = %s", (releasename,))
                    result = cursor.fetchone()

                    if result:
                        # Release exists - update it
                        if result[0] == 1:
                            irc.reply(f"Release {releasename} is already nuked.")
                            return

                        cursor.execute(
                            "UPDATE releases SET nuked = %s, reason = %s, nukenet = %s WHERE releasename = %s",
                            (1, reason, nukenet, releasename),
                        )
                        conn.commit()

                        if cursor.rowcount > 0:
                            self.announce_nuke_status(irc, releasename, reason, nukenet, 4)
                    else:
                        # Release not found - add to new_nukes instead
                        group = releasename.split('-')[-1] if '-' in releasename else None
                        
                        try:
                            cursor.execute(
                                "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (%s, %s, %s, %s, 4)",
                                (releasename, reason, nukenet, group),
                            )
                            conn.commit()
                            irc.reply(f"Release {releasename} not in database - added to pending nukes. Use +newnukes to review.")
                        except Exception as e:
                            irc.reply(f"Release {releasename} already in pending nukes")
                            self.log.error(f"Error adding to new_nukes: {e}")

            except Exception as e:
                self.log.error(f"Delpre error: {e}")
                irc.reply(f"Error processing nuke: {str(e)}") 

    # =========
    # UNDELPRE
    # =========
    def handle_addundelpre(self, irc, msg, args):
        """Threadpool-based undelpre handler"""
        if msg.nick not in ["CTW_PRE", "klapvogn"]:
            return
            
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
        """Enhanced undelpre handler - adds to new_nukes if not in main database"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Check if release exists in main releases table
                cursor.execute("SELECT nuked FROM releases WHERE releasename = %s", (releasename,))
                result = cursor.fetchone()

                if result:
                    # Release exists - update it
                    if result[0] == 2:
                        irc.reply(f"Release {releasename} is already unnuked.")
                        return

                    cursor.execute(
                        "UPDATE releases SET nuked = %s, reason = %s, nukenet = %s WHERE releasename = %s",
                        (2, reason, nukenet, releasename),
                    )
                    conn.commit()

                    if cursor.rowcount > 0:
                        self.announce_nuke_status(irc, releasename, reason, nukenet, 5)
                else:
                    # Release not found - add to new_nukes instead
                    group = releasename.split('-')[-1] if '-' in releasename else None
                    
                    try:
                        cursor.execute(
                            "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (%s, %s, %s, %s, 5)",
                            (releasename, reason, nukenet, group),
                        )
                        conn.commit()
                        irc.reply(f"Release {releasename} not in database - added to pending unnukes. Use +newunnukes to review.")
                    except Exception as e:
                        irc.reply(f"Release {releasename} already in pending undelpre")
                        self.log.error(f"Error adding to new_nukes: {e}")

        except Exception as e:
            self.log.error(f"Unnuke error: {e}")
            irc.reply(f"Error processing undelpre: {str(e)}")                               

    # =========
    # ADDUNNUKE
    # =========
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

    # ==========================================================
    # Add unnukes that is not in database to an new_nukes database
    # ==========================================================
    def _unnuke_thread(self, irc, releasename, reason, nukenet):
        """Enhanced unnuke handler - adds to new_nukes if not in main database"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Check if release exists in main releases table
                cursor.execute("SELECT nuked FROM releases WHERE releasename = %s", (releasename,))
                result = cursor.fetchone()

                if result:
                    # Release exists - update it
                    if result[0] == 2:
                        irc.reply(f"Release {releasename} is already unnuked.")
                        return

                    cursor.execute(
                        "UPDATE releases SET nuked = %s, reason = %s, nukenet = %s WHERE releasename = %s",
                        (2, reason, nukenet, releasename),
                    )
                    conn.commit()

                    if cursor.rowcount > 0:
                        self.announce_nuke_status(irc, releasename, reason, nukenet, 2)
                else:
                    # Release not found - add to new_nukes instead
                    group = releasename.split('-')[-1] if '-' in releasename else None
                    
                    try:
                        cursor.execute(
                            "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (%s, %s, %s, %s, 2)",
                            (releasename, reason, nukenet, group),
                        )
                        conn.commit()
                        irc.reply(f"Release {releasename} not in database - added to pending unnukes. Use +newunnukes to review.")
                    except Exception as e:
                        irc.reply(f"Release {releasename} already in pending unnukes")
                        self.log.error(f"Error adding to new_nukes: {e}")

        except Exception as e:
            self.log.error(f"Unnuke error: {e}")
            irc.reply(f"Error processing unnuke: {str(e)}")

    # =======
    # MODNUKE
    # =======
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

    # ==========================================================
    # Add modnukes that is not in database to an new_nukes database
    # ==========================================================
    def _modnuke_thread(self, irc, releasename, reason, nukenet):
            """Enhanced modnuke handler - adds to new_nukes if not in main database"""
            try:
                with self.db_connection() as conn:
                    cursor = conn.cursor()

                    # Check if release exists in main releases table
                    cursor.execute("SELECT nuked FROM releases WHERE releasename = %s", (releasename,))
                    result = cursor.fetchone()

                    if result:
                        # Release exists - update it
                        if result[0] == 1:
                            irc.reply(f"Release {releasename} is already nuked.")
                            return

                        cursor.execute(
                            "UPDATE releases SET nuked = %s, reason = %s, nukenet = %s WHERE releasename = %s",
                            (1, reason, nukenet, releasename),
                        )
                        conn.commit()

                        if cursor.rowcount > 0:
                            self.announce_nuke_status(irc, releasename, reason, nukenet, 3)
                    else:
                        # Release not found - add to new_nukes instead
                        group = releasename.split('-')[-1] if '-' in releasename else None
                        
                        try:
                            cursor.execute(
                                "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (%s, %s, %s, %s, 3)",
                                (releasename, reason, nukenet, group),
                            )
                            conn.commit()
                            irc.reply(f"Release {releasename} not in database - added to pending nukes. Use +newmodnukes to review.")
                        except Exception as e:
                            irc.reply(f"Release {releasename} already in pending modnukes")
                            self.log.error(f"Error adding to new_nukes: {e}")

            except Exception as e:
                self.log.error(f"Modnuke error: {e}")
                irc.reply(f"Error processing modnuke: {str(e)}")

    # =======
    # ADDINFO
    # =======
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
                    "UPDATE releases SET files = %s, size = %s WHERE releasename = %s",
                    (files, size, releasename),
                )
                conn.commit()

        except Exception as e:
            self.log.error(f"Addinfo error: {e}")

    # ========================
    # ANNOUNCEMENTS TO CHANNEL
    # ========================
    def announce_pre(self, irc, releasename, section):
        """Announce new release to the #omgwtfnzbs.pre channel on another network."""
        #self.log.info(f"Announcing release: {section} {releasename}")
        # Define the target channel
        #target_channel = "#bot"
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

    # =====================
    # UNIFIED ANNOUNCEMENTS
    # =====================
    def announce_nuke_status(self, irc, releasename, reason, nukenet, nuke_type):
        """Unified nuke announcement handler"""
        nuke_status = {
            '1': ("\x0304NUKE\x03", "\x0304"),
            '2': ("\x0303UNNUKE\x03", "\x0303"),
            '3': ("\x0305MODNUKE\x03", "\x0305"),
            '4': ("\x03035DELPRE\x03", "\x0305"),
            '5': ("\x03045UNDELPRE\x03", "\x0304")
        }
        
        # Convert nuke_type to string for dictionary lookup
        nuke_type_str = str(nuke_type)
        
        name, color = nuke_status.get(nuke_type_str, ("UNKNOWN", "05"))
        announcement = f"[ \x03{color}{name}\x03 ] {releasename} [ \x03{color}{reason}\x03 ] => \x03{color}{nukenet}\x03"
        
        if irc_state := self._get_target_irc_state():
            #irc_state.queueMsg(ircmsgs.privmsg("#bot", announcement))
            irc_state.queueMsg(ircmsgs.privmsg("#omgwtfnzbs.pre", announcement))

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
        
        self.log.info(f"DEBUG: Received message in {channel}: {text}")
        
        # Store for command handlers
        self.irc = irc
        self.msg = msg
              
        # Process commands in any channel
        if any(text.startswith(cmd) for cmd in self.command_handlers):
            self._process_command(text)
        else:
            self.log.debug("Message doesn't match any patterns, skipping")

        # Clean expired release cache entries
        self._clean_expired_cache_entries()

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
    @lru_cache(maxsize=1)
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
            print(f"Database stats error: {e}")
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
                        time_ago = self.format_time_ago(unixtime)
                        
                        message = (
                            f"[ \x0305PENDING NUKE\x03 ] ID: {nuke_id_val} | {releasename} | "
                            f"Reason: {reason} | Network: {nukenet} | Time: {time_ago}"
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
                        time_ago = self.format_time_ago(unixtime)
                        message = f"[ ID: {nuke_id} ] [ Release: {releasename} ] [ Reason: {reason} => Network: {nukenet} ] [ Time: {time_ago} ]"
                        irc.reply(message, private=True)

        except Exception as e:
            self.log.error(f"Error in newnukes: {e}")
            irc.reply(f"Error retrieving pending nukes: {str(e)}")

    newnukes = commands.wrap(newnukes, [optional('text')])

    # ===============================================================================
    # Use this to convert timestamp to unix timestamp https://www.epochconverter.com/
    # ================================================================================
    def movenuke(self, irc, msg, args):
        """<id> <unixtime> [<section>] - Move a pending nuke to main database with timestamp"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        # Parse arguments from rest (args is a list)
        parts = args if isinstance(args, list) else args.split()
        
        if len(parts) < 2:
            irc.reply("Usage: +movenuke <id> <unixtime> [<section>]")
            return

        try:
            nuke_id = int(parts[0])
            new_unixtime = int(parts[1])
            section = ' '.join(parts[2:]) if len(parts) > 2 else None
        except ValueError:
            irc.reply("ID and unixtime must be integers")
            return

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Fetch the pending nuke
                cursor.execute("""
                    SELECT releasename, reason, nukenet, grp
                    FROM new_nukes
                    WHERE id = ?
                """, (nuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Pending nuke ID {nuke_id} not found")
                    return

                releasename, reason, nukenet, grp = result

                # Insert into main releases table
                cursor.execute(
                    "INSERT INTO releases (releasename, section, unixtime, grp, reason, nukenet, nuked) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet),
                )

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (nuke_id,))
                conn.commit()

                irc.reply(f"Moved {releasename} to main database with unixtime {new_unixtime}")

        except Exception as e:
            self.log.error(f"Error in movenuke: {e}")
            irc.reply(f"Error moving nuke: {str(e)}")

    def deletenuke(self, irc, msg, args):
        """<id> - Delete a pending nuke"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        if not args:
            irc.reply("Usage: +deletenuke <id>")
            return

        try:
            nuke_id = int(args[0])
        except ValueError:
            irc.reply("ID must be an integer")
            return

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (nuke_id,))
                conn.commit()

                if cursor.rowcount > 0:
                    irc.reply(f"Deleted pending nuke ID: {nuke_id}")
                else:
                    irc.reply(f"Pending nuke ID: {nuke_id} not found")

        except Exception as e:
            self.log.error(f"Error in deletenuke: {e}")
            irc.reply(f"Error deleting nuke: {str(e)}")

    deletenuke = commands.wrap(deletenuke, ['text'])

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
                        time_ago = self.format_time_ago(unixtime)
                        
                        message = (
                            f"[ \x0303PENDING UNNUKE\x03 ] ID: {unnuke_id_val} | {releasename} | "
                            f"Reason: {reason} | Network: {nukenet} | Time: {time_ago}"
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
                        time_ago = self.format_time_ago(unixtime)
                        #message = f"[ ID: {unnuke_id} ] [ {releasename} ] [ {reason} => {nukenet} ] [ {time_ago} ]"
                        message = f"[ ID: {unnuke_id} ] [ Release: {releasename} ] [ Reason: {reason} => Network: {nukenet} ] [ Time: {time_ago} ]"
                        irc.reply(message, private=True)

        except Exception as e:
            self.log.error(f"Error in newunnukes: {e}")
            irc.reply(f"Error retrieving pending unnukes: {str(e)}")

    newunnukes = commands.wrap(newunnukes, [optional('text')])

    # ===============================================================================
    # Use this to convert timestamp to unix timestamp https://www.epochconverter.com/
    # ================================================================================
    def moveunnuke(self, irc, msg, args):
        """<id> <unixtime> [<section>] - Move a pending unnuke to main database with timestamp"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        # Parse arguments from rest (args is a list)
        parts = args if isinstance(args, list) else args.split()
        
        if len(parts) < 2:
            irc.reply("Usage: +moveunnuke <id> <unixtime> [<section>]")
            return

        try:
            unnuke_id = int(parts[0])
            new_unixtime = int(parts[1])
            section = ' '.join(parts[2:]) if len(parts) > 2 else None
        except ValueError:
            irc.reply("ID and unixtime must be integers")
            return

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Fetch the pending unnuke
                cursor.execute("""
                    SELECT releasename, reason, nukenet, grp
                    FROM new_nukes
                    WHERE id = %s
                """, (unnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Pending unnuke ID {unnuke_id} not found")
                    return

                releasename, reason, nukenet, grp = result

                cursor.execute(
                    "INSERT INTO releases (releasename, section, unixtime, grp, reason, nukenet, nuked) VALUES (%s, %s, %s, %s %s, %s, 2)",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet),
                )

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (unnuke_id,))
                conn.commit()

                irc.reply(f"Moved {releasename} to main database with unixtime {new_unixtime}")

        except Exception as e:
            self.log.error(f"Error in moveunnuke: {e}")
            irc.reply(f"Error moving unnuke: {str(e)}")

    def deleteunnuke(self, irc, msg, args):
        """<id> - Delete a pending nuke"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        if not args:
            irc.reply("Usage: +deleteunnuke <id>")
            return

        try:
            unnuke_id = int(args[0])
        except ValueError:
            irc.reply("ID must be an integer")
            return

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (unnuke_id,))
                conn.commit()

                if cursor.rowcount > 0:
                    irc.reply(f"Deleted pending nuke ID: {unnuke_id}")
                else:
                    irc.reply(f"Pending nuke ID: {unnuke_id} not found")

        except Exception as e:
            self.log.error(f"Error in deleteunnuke: {e}")
            irc.reply(f"Error deleting nuke: {str(e)}")

    deleteunnuke = commands.wrap(deleteunnuke, ['text'])

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
                        time_ago = self.format_time_ago(unixtime)
                        
                        message = (
                            f"[ \x0305PENDING MODNUKE\x03 ] ID: {modnuke_id_val} | {releasename} | "
                            f"Reason: {reason} | Network: {nukenet} | Time: {time_ago}"
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
                        time_ago = self.format_time_ago(unixtime)
                        #message = f"[ ID: {nuke_id} ] [ {releasename} ] [ {reason} => {nukenet} ] [ {time_ago} ]"
                        message = f"[ ID: {modnuke_id} ] [ Release: {releasename} ] [ Reason: {reason} => Network: {nukenet} ] [ Time: {time_ago} ]"
                        irc.reply(message, private=True)

        except Exception as e:
            self.log.error(f"Error in newmodnukes: {e}")
            irc.reply(f"Error retrieving pending modnukes: {str(e)}")

    newmodnukes = commands.wrap(newmodnukes, [optional('text')])

    # ===============================================================================
    # Use this to convert timestamp to unix timestamp https://www.epochconverter.com/
    # ================================================================================
    def movemodnuke(self, irc, msg, args):
        """<id> <unixtime> [<section>] - Move a pending modnuke to main database with timestamp"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        # Parse arguments from rest (args is a list)
        parts = args if isinstance(args, list) else args.split()
        
        if len(parts) < 2:
            irc.reply("Usage: +movemodnuke <id> <unixtime> [<section>]")
            return

        try:
            modnuke_id = int(parts[0])
            new_unixtime = int(parts[1])
            section = ' '.join(parts[2:]) if len(parts) > 2 else None
        except ValueError:
            irc.reply("ID and unixtime must be integers")
            return

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()

                # Fetch the pending modnuke
                cursor.execute("""
                    SELECT releasename, reason, nukenet, grp
                    FROM new_nukes
                    WHERE id = %s
                """, (modnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Pending modnuke ID: {modnuke_id} not found")
                    return

                releasename, reason, nukenet, grp = result

                cursor.execute(
                    "INSERT INTO releases (releasename, section, unixtime, grp, reason, nukenet, nuked) VALUES (%s, %s, %s, %s, %s, %s, 3)",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet),
                )

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (modnuke_id,))
                conn.commit()

                irc.reply(f"Moved {releasename} to main database with unixtime {new_unixtime}")

        except Exception as e:
            self.log.error(f"Error in movemodnuke: {e}")
            irc.reply(f"Error moving movemodnuke: {str(e)}")


    def deletemodnukes(self, irc, msg, args, modnuke_id_int):
        """<id> - Delete a pending modnuke"""
        
        # Security check
        if msg.nick != 'klapvogn':
            irc.reply("Error: You do not have permission to use this command.")
            return

        try:
            modnuke_id_int = int(modnuke_id_int)
        except ValueError:
            irc.reply("ID must be an integer")
            return

        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM new_nukes WHERE id = %s", (modnuke_id_int,))
                conn.commit()

                if cursor.rowcount > 0:
                    irc.reply(f"Deleted pending modnuke ID: {modnuke_id_int}")
                else:
                    irc.reply(f"Pending modnuke ID: {modnuke_id_int} not found")

        except Exception as e:
            self.log.error(f"Error in modnukes: {e}")
            irc.reply(f"Error deleting modnukes: {str(e)}")

    deletemodnukes = commands.wrap(deletemodnukes, ['text'])    

    def pending(self, irc, msg, args):
        """Show counts of pending nukes, unnukes, and modnukes"""
        try:
            with self.db_connection() as conn:
                cursor = conn.cursor()
                
                # Count each type based on the nuked column
                cursor.execute("SELECT nuked, COUNT(*) FROM new_nukes GROUP BY nuked")
                results = cursor.fetchall()
                
                # Initialize counts
                counts = {1: 0, 2: 0, 3: 0}
                for nuked_type, count in results:
                    if nuked_type in counts:
                        counts[nuked_type] = count
                
                irc.reply(f"\x0305Nukes\x03: {counts[1]} | \x0303Unnukes\x03: {counts[2]} | \x0305Modnukes\x03: {counts[3]}")

        except Exception as e:
            self.log.error(f"Error in pending: {e}")
            irc.reply(f"Error retrieving pending counts: {str(e)}")

    pending = commands.wrap(pending, [])
    
    # ================
    # HELPER FUNCTIONS
    # ================
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
        if n is None:
            return "0"
        return f"{n:,}"

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