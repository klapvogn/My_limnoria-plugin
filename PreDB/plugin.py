import sqlite3
import time
import threading
import json
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
        "X264-UHD": "\00311X264-UHD\003",   
        "X264-UHD-NL": "\00311X264-UHD-NL\003",
        "X264-UHD-IT": "\00311X264-UHD-IT\003",  
        "X264-UHD-PL": "\00311X264-UHD-PL\003",
        "X264-UHD-FR": "\00311X264-UHD-FR\003",
        "X264-UHD-DE": "\00311X264-UHD-DE\003",
        "X264-UHD-CZ": "\00311X264-UHD-CZ\003",
        "X264-UHD-ES": "\00311X264-UHD-ES\003",        
        "X264-UHD-SP": "\00311X264-UHD-SP\003",
         "X264-UHD-NORDiC": "\00311X264-UHD-NORDiC\003",  

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
            # Sanitize the passphrase to prevent SQL injection
            # Remove any single quotes and limit length
            sanitized_passphrase = self.passphrase.replace("'", "").replace('"', '')[:100]
            conn.execute(f"PRAGMA key = '{sanitized_passphrase}'")
            conn.execute("PRAGMA cache_size = -200000")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA journal_mode = WAL")
            yield conn
        except Exception as e:
            conn.close()
            raise e
        finally:
            conn.close()

    # ====================
    # DATABASE OPERATIONS
    # ====================
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
                        files INTEGER DEFAULT NULL,
                        size INTEGER DEFAULT NULL,
                        grp TEXT,
                        genre TEXT,
                        nuked INTEGER DEFAULT NULL,
                        reason TEXT,
                        nukenet TEXT
                    )
                ''')
                
                # Create new_nukes table for unknown releases
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS new_nukes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        releasename TEXT UNIQUE NOT NULL,
                        section TEXT,                               
                        unixtime INTEGER DEFAULT (strftime('%s', 'now')),
                        files INTEGER DEFAULT NULL,
                        size INTEGER DEFAULT NULL,
                        grp TEXT,                                                              
                        nuked INTEGER DEFAULT NULL,                      
                        reason TEXT,
                        nukenet TEXT
                    )
                ''')
                
                # Create indexes for releases table
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_releasename ON releases(releasename)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_section ON releases(section)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_grp ON releases(grp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_nuked ON releases(nuked)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_unixtime ON releases(unixtime)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_grp_nuked ON releases(grp, nuked)')
                
                # Create indexes for new_nukes table
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_new_nukes_releasename ON new_nukes(releasename)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_new_nukes_unixtime ON new_nukes(unixtime)')
                
                conn.commit()           
    
    # ========================
    # CACHED HTTP REQUESTS
    # ========================
    @lru_cache(maxsize=1000)
    def get_cached_content(self, url):
        """Cached HTTP GET with timeout - returns content instead of response object"""
        try:
            response = self.session.get(url, timeout=5)
            return response.text if response.status_code == 200 else None
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
                    return f"[ \x033NFO\x03: {shortened} ]"
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
                    return f"[ \x033SFV\x03: {shortened} ]"
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
                    WHERE releasename = ?
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
            # Connect to the encrypted database using SQLCipher
            with self.db_connection() as conn:
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
            - Requires SQLITE_PASSPHRASE environment variable for database access
        
        Process:
            1. Verify user authorization
            2. Validate argument count
            3. Retrieve database passphrase from environment
            4. Update section in encrypted SQLite database
            5. Provide appropriate success/error feedback
        
        Raises:
            sqlcipher.DatabaseError: If SQLCipher-specific database operations fail
            sqlite3.Error: If general SQLite database operations fail
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

        # Retrieve the passphrase from environment variable
        passphrase = os.getenv("SQLITE_PASSPHRASE")
        if not passphrase:
            irc.reply("Error: Database passphrase is not set.")
            return

        try:
            # Connect to the database using SQLCipher
            with self.db_connection() as conn:
                
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
                if release != "*":
                    irc.reply(f"[ \x0305Nothing found, that makes me a sad pre bot :-(\x03 ]")
                else:
                    irc.reply("\x0305No releases found.\x03")
                return

            # Unpack and process result
            releasename, section, unixtime, files, size, grp, genre, nuked, reason, nukenet = result
            # Lookup the section color
            section_formatted = self.section_colors.get(section, section)  # Default to section name if not found 
            # Get time_ago string and color
            time_ago_str, time_color = self.format_time_ago(unixtime)
            pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
            
            # Apply the same color to both time displays
            time_ago = f"{time_color}{time_ago_str}\x03"
            pretime_colored = f"{time_color}{pretime_formatted}\x03"
            
            # Get links in parallel
            nfo_text, sfv_text, srr_text = self.get_all_links(releasename)
            # Info and section        
            info_string = f"[ \x033INFO\x03: {size} MB, {files} Files ] " if size and files else ""
            section_and_genre = f"[ {section_formatted} / {genre} ]" if genre and genre.lower() != 'null' else f"[ {section_formatted} ]"

            nuke_status = {
                1: ("\x0305Nuked\x03", "\x0305"),
                2: ("\x0303UnNuked\x03", "\x0303"),
                3: ("\x0305ModNuked\x03", "\x0305")
            }
            status, color = nuke_status.get(nuked, ("", ""))
            nuked_details = f"[ {status}: {color}{reason or 'No reason'}\x03 => {color}{nukenet or 'Unknown'}\x03 ]" if status else ""
                     
            # Build response
            message = (
                f"\x033[ PRED ]\x03 [ {releasename} ] [ \x033TIME\x03: {time_ago} / {pretime_colored} ] "
                f"in {section_and_genre} {info_string}{nuked_details}"
                f"{nfo_text}{sfv_text}{srr_text}"
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
                    WHERE releasename LIKE ?
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
                    1: ("\x0305Nuked\x03", "\x0305"),
                    2: ("\x0303UnNuked\x03", "\x0303"),
                    3: ("\x0305ModNuked\x03", "\x0305")
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
                        SUM(nuked = 1) AS nukes,
                        SUM(nuked = 2) AS unnukes,
                        SUM(nuked = 3) AS modnukes,
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

                total, nukes, unnukes, modnukes, first_time, last_time, first_release, last_release = result
                
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
                        f"[ \x0305NUKES\03: {nukes} ] [ \x0305MODNUKES\03: {modnukes} ] [ \x033UNNUKES\03: {unnukes} ]")
                
                if first_release:
                    irc.reply(f"\x037[ FIRST RELEASE\x03 ] {first_release} [ Time: {first_time_fmt} ]")
                if last_release:
                    irc.reply(f"\x033[ LAST RELEASE\x03 ] {last_release} [ Time: {last_time_fmt} ]")

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
                    WHERE nuked = 1
                """
                params = []

                if groupname:
                    query += " AND grp = ?"
                    params.append(groupname)

                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                if groupname:
                    irc.reply(f"\x0305No nuked releases found for group\x03: {groupname}")
                else:
                    irc.reply("\x0305No nuked releases found.")
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
                    WHERE nuked = 2
                """
                params = []

                # Add group filter if provided
                if groupname:
                    query += " AND grp = ?"
                    params.append(groupname)

                # Get the most recent result
                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                msg = f"No unnuked releases found{' for group ' + groupname if groupname else ''}"
                irc.reply(f"\x0305{msg}\x03")
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
                    WHERE nuked = 3
                """
                params = []

                # Add group filter if provided
                if groupname:
                    query += " AND grp = ?"
                    params.append(groupname)

                # Get the most recent result
                query += " ORDER BY unixtime DESC LIMIT 1"
                cursor.execute(query, params)
                result = cursor.fetchone()

            if not result:
                msg = f"No modnuke releases found{' for group ' + groupname if groupname else ''}"
                irc.reply(f"\x0305{msg}\x03")
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
                f"[ \x0304MODNUKED\x03 ] [ {releasename} ] pred [ {time_ago} / {pretime_colored} ] "
                f"in [ {section_formatted} ] [ \x0304{reason or 'Unknown reason'}\x03 => \x0304{nukenet or 'Unknown network'}\x03 ]"
            )
            
        except Exception as e:
            self.log.error(f"Error in lastmodnuke: {e}")
            irc.reply(f"Error retrieving unnuke data: {str(e)}")            

    # =======
    # SECTION
    # =======          
    def section(self, irc, msg, args, section=None):
        """
        Retrieve the 10 most recent nuked releases, optionally filtered by section.
        
        This command queries the database for nuked releases and returns detailed
        information including release name, timestamp, section, nuke reason, and
        additional metadata. Results are sent via private message.
        
        Parameters
        ----------
        section : str, optional
            The section to filter by (e.g., '0DAY', 'APPS', 'GAMES'). If not provided,
            returns nuked releases from all sections.
            
        Returns
        -------
        None
            Sends results as private messages to the user with formatted release information.
            
        Examples
        --------
        !section
            Returns 10 most recent nuked releases from all sections
            
        !section 0DAY
            Returns 10 most recent nuked releases from the 0DAY section
            
        Notes
        -----
        - Results are ordered by most recent first (based on unixtime)
        - Limited to 10 results maximum
        - Includes colored formatting for better readability in IRC
        - Shows time ago and absolute timestamp for each release
        - Includes size and file count when available
        """
        try:
            with self.db_connection() as conn:
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
                irc.reply(f"\x0305Nothing found that makes me a sad pre bot :-(\x03")
                return
            
            # Notify the user about sending results
            irc.reply(f"PM'ing last 10 results to {msg.nick}")

            # Process all results efficiently
            messages = []
            for result in results:
                releasename, unixtime, section, reason, nukenet, size, files = result
                section_formatted = self.section_colors.get(section, section)
                # Get time_ago string and color
                time_ago_str, time_color = self.format_time_ago(unixtime)
                pretime_formatted = datetime.utcfromtimestamp(unixtime).strftime("%Y-%m-%d %H:%M:%S GMT")
                
                # Apply the same color to both time displays
                time_ago = f"{time_color}{time_ago_str}\x03"
                pretime_colored = f"{time_color}{pretime_formatted}\x03"
                
                # Build info string if data exists
                info_string = f"[ INFO: {size} MB, {files} Files ] " if size and files else ""
                
                message = (
                    f"[ \x033PRED\x03 ] [ {releasename} ] "
                    f"pred [ {time_ago} / {pretime_colored} ] "
                    f"in [ {section_formatted} ] "
                    f"{info_string}"
                    f"[ \x0305{reason or 'Unknown reason'}\x03 => \x0305{nukenet or 'Unknown network'}\x03 ]"
                )
                messages.append(message)

            # Send all messages to the user
            for message in messages:
                irc.reply(message, private=True)

        except Exception as e:
            self.log.error(f"Error in section: {e}")
            irc.reply(f"Error retrieving section data: {str(e)}")
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

    # =======
    # ADDNUKE
    # =======
    @wrap(['text', 'text', 'text'])
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

    # ==========================================================
    # Add nukes that is not in database to an new_nukes database
    # ==========================================================
    def _nuke_thread(self, irc, releasename, reason, nukenet):
            """Enhanced nuke handler - adds to new_nukes if not in main database"""
            try:
                with self.db_connection() as conn:
                    cursor = conn.cursor()

                    # Check if release exists in main releases table
                    cursor.execute("SELECT nuked FROM releases WHERE releasename = ?", (releasename,))
                    result = cursor.fetchone()

                    if result:
                        # Release exists - update it
                        if result[0] == 1:
                            irc.reply(f"Release {releasename} is already nuked.")
                            return

                        cursor.execute(
                            "UPDATE releases SET nuked = ?, reason = ?, nukenet = ? WHERE releasename = ?",
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
                                "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (?, ?, ?, ?, 1)",
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

    # =========
    # ADDUNNUKE
    # =========
    @wrap(['text', 'text', 'text'])
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
                cursor.execute("SELECT nuked FROM releases WHERE releasename = ?", (releasename,))
                result = cursor.fetchone()

                if result:
                    # Release exists - update it
                    if result[0] == 2:
                        irc.reply(f"Release {releasename} is already unnuked.")
                        return

                    cursor.execute(
                        "UPDATE releases SET nuked = ?, reason = ?, nukenet = ? WHERE releasename = ?",
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
                            "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (?, ?, ?, ?, 2)",
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
    @wrap(['text', 'text', 'text'])
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
                    cursor.execute("SELECT nuked FROM releases WHERE releasename = ?", (releasename,))
                    result = cursor.fetchone()

                    if result:
                        # Release exists - update it
                        if result[0] == 1:
                            irc.reply(f"Release {releasename} is already nuked.")
                            return

                        cursor.execute(
                            "UPDATE releases SET nuked = ?, reason = ?, nukenet = ? WHERE releasename = ?",
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
                                "INSERT INTO new_nukes (releasename, reason, nukenet, grp, nuked) VALUES (?, ?, ?, ?, 3)",
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
                    "UPDATE releases SET files = ?, size = ? WHERE releasename = ?",
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
            if len(args) >= 2:
                self.handle_addpre(irc, msg, args)
        elif text.startswith("!info"):
            args = text.split()[1:]  # Extract arguments after the command
            if len(args) >= 3:
                self.handle_addinfo(irc, msg, args)
        elif text.startswith("!nuke"):
            args = text.split()[1:]  # Extract arguments after the command
            if len(args) >= 3:
                self.handle_addnuke(irc, msg, args)
        elif text.startswith("!modnuke"):
            args = text.split()[1:]  # Extract arguments after the command
            if len(args) >= 3:
                self.handle_addmodnuke(irc, msg, args)            
        elif text.startswith("!unnuke"):
            args = text.split()[1:]  # Extract arguments after the command
            if len(args) >= 3:
                self.handle_addunnuke(irc, msg, args)  

    # ===================
    # DATABASE STATISTICS
    # ===================
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
                            WHERE id = ?
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
                    "INSERT INTO releases (releasename, section, unixtime, grp, reason, nukenet, nuked) VALUES (?, ?, ?, ?, ?, ?, 1)",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet),
                )

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = ?", (nuke_id,))
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
                cursor.execute("DELETE FROM new_nukes WHERE id = ?", (nuke_id,))
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
                            WHERE id = ?
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
                    WHERE id = ?
                """, (unnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Pending unnuke ID {unnuke_id} not found")
                    return

                releasename, reason, nukenet, grp = result

                cursor.execute(
                    "INSERT INTO releases (releasename, section, unixtime, grp, reason, nukenet, nuked) VALUES (?, ?, ?, ?, ?, ?, 2)",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet),
                )

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = ?", (unnuke_id,))
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
                cursor.execute("DELETE FROM new_nukes WHERE id = ?", (unnuke_id,))
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
                            WHERE id = ?
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
                    WHERE id = ?
                """, (modnuke_id,))
                result = cursor.fetchone()

                if not result:
                    irc.reply(f"Pending modnuke ID: {modnuke_id} not found")
                    return

                releasename, reason, nukenet, grp = result

                cursor.execute(
                    "INSERT INTO releases (releasename, section, unixtime, grp, reason, nukenet, nuked) VALUES (?, ?, ?, ?, ?, ?, 3)",
                    (releasename, section or "UNKNOWN", new_unixtime, grp, reason, nukenet),
                )

                # Delete from new_nukes
                cursor.execute("DELETE FROM new_nukes WHERE id = ?", (modnuke_id,))
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
                cursor.execute("DELETE FROM new_nukes WHERE id = ?", (modnuke_id_int,))
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
            irc.reply(message, private=True)


Class = PreDB
