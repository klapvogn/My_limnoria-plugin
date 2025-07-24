import os
import time
import datetime
import supybot.world  # Import the supybot world
import supybot.ircmsgs as ircmsgs  # Import IRC message functions
import sqlite3
import sqlcipher3
from sqlite3 import Error
from supybot.commands import *
from collections import defaultdict
from supybot import utils, plugins, ircutils, callbacks, conf
from functools import wraps

class WordCounter(callbacks.Plugin):
    def __init__(self, irc):
        super().__init__(irc)
        # Initialize data structures
        self.log.info("Loading WordCounter")
        self.last_messages = {}
        self.wordstats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'lines': 0, 'words': 0}))))
        self.last_save_time = time.time()
        self.last_midnight_save = None  # Track last midnight save
        
        # Configuration for channels to track and bots to ignore
        self.tracked_channels = ['#bot', '#omgwtfnzbs.chat']  # Add your channels here
        self.bot_nicks_to_ignore = ['CTW_PRE', 'omgwtfnzb']  # Add bot nicks to ignore here
        
        # Set up database directory and file path
        self.passphrase = os.getenv("SQLITE_PASSPHRASE")
        self.data_dir = conf.supybot.directories.data.dirize("WordCounter")
        self.db_file = os.path.join(self.data_dir, "wordcounter_stats.db")
        
        # Create directory if it doesn't exist
        if not os.path.exists(self.data_dir):
            try:
                os.makedirs(self.data_dir)
                self.log.info(f"Created directory: {self.data_dir}")
            except OSError as e:
                self.log.error(f"Failed to create directory: {e}")
        
        # Initialize database
        self._init_db()
        # Load existing stats
        self._load_stats()

    def _init_db(self):
        """Initialize the encrypted SQLite database."""
        try:
            # Create or connect to the database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Set encryption key (you should use a more secure way to store/get this)
            conn.execute(f"PRAGMA key = '{self.passphrase}'")
            
            # Create tables if they don't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS word_stats (
                    channel TEXT,
                    year INTEGER,
                    month INTEGER,
                    nick TEXT,
                    lines INTEGER,
                    words INTEGER,
                    PRIMARY KEY (channel, year, month, nick)
                )
            ''')
            
            # Test encryption by querying
            cursor.execute("SELECT count(*) FROM word_stats")
            conn.commit()
            conn.close()
            self.log.info("Database initialized successfully")
        except Error as e:
            self.log.error(f"Error initializing database: {e}")

    def _get_connection(self):
        """Get a database connection with encryption key set."""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            conn.execute(f"PRAGMA key = '{self.passphrase}'")
            return conn
        except Error as e:
            self.log.error(f"Error getting database connection: {e}")
            return None

    def _load_stats(self):
        """Load statistics from the database."""
        self.log.debug("Loading stats from database")
        try:
            conn = self._get_connection()
            if conn is None:
                return
                
            cursor = conn.cursor()
            cursor.execute('SELECT channel, year, month, nick, lines, words FROM word_stats')
            
            # Clear current stats
            self.wordstats.clear()
            
            # Load data from database
            for row in cursor.fetchall():
                channel, year, month, nick, lines, words = row
                self.wordstats[channel][year][month][nick] = {
                    'lines': lines,
                    'words': words
                }
            
            conn.close()
            self.log.info("Successfully loaded stats from database")
        except Error as e:
            self.log.error(f"Error loading stats from database: {e}")

    def _save_stats(self):
        """Save statistics to the database."""
        self.log.debug("Saving stats to database")
        try:
            conn = self._get_connection()
            if conn is None:
                return
                
            cursor = conn.cursor()
            
            # Begin transaction
            cursor.execute('BEGIN TRANSACTION')
            
            # Clear existing data (we're doing a full replace for simplicity)
            cursor.execute('DELETE FROM word_stats')
            
            # Insert current stats
            for channel, years in self.wordstats.items():
                for year, months in years.items():
                    for month, users in months.items():
                        for nick, counts in users.items():
                            cursor.execute('''
                                INSERT INTO word_stats (channel, year, month, nick, lines, words)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (channel, year, month, nick, counts['lines'], counts['words']))
            
            # Commit transaction
            conn.commit()
            conn.close()
            self.log.debug("Successfully saved stats to database")
        except Error as e:
            self.log.error(f"Error saving stats to database: {e}")
            if conn:
                conn.rollback()
                conn.close()

    def doPrivmsg(self, irc, msg):
        channel = msg.args[0]
        # Convert both stored channels and incoming channel to lowercase for comparison
        if not ircutils.isChannel(channel) or channel.lower() not in [c.lower() for c in self.tracked_channels]:
            return
            
        nick = msg.nick
        text = msg.args[1]

        # Skip commands starting with +
        if text.startswith('+'):
            return        
        
        # Skip messages from bots in the ignore list
        if nick in self.bot_nicks_to_ignore:
            return
        
        # Duplicate message check
        now = time.time()
        last_time = self.last_messages.get(nick, 0)
        if now - last_time < 1 and text == self.last_messages.get((nick, 'text'), None):
            return
            
        self.last_messages[nick] = now
        self.last_messages[(nick, 'text')] = text
        
        # Update stats
        current_time = datetime.datetime.now()
        year = current_time.year
        month = current_time.month
        
        self.wordstats[channel][year][month][nick]['lines'] += 1
        self.wordstats[channel][year][month][nick]['words'] += len(text.split())
        
        # Save conditions (both will work independently)
        save_triggered = False
        
        # 1. Save at midnight (with 60-second check)
        if current_time.hour == 0 and current_time.minute == 0:
            if now - self.last_save_time >= 60:  # Ensure we only save once per minute
                self._save_stats()
                self.last_save_time = now
                save_triggered = True
        
        # 2. Save every 24 hours (unless we already saved at midnight)
        if not save_triggered and (now - self.last_save_time >= 86400):  # 86400 seconds = 24 hours
            self._save_stats()
            self.last_save_time = now

    def stats(self, irc, msg, args, opt_channel, opt_year_month):
        """[<channel>] [<year-month>]
        
        Shows statistics for the specified channel and year-month.
        Format: +stats [channel] [YYYY-MM]
        Examples: 
        +stats (in channel)
        +stats #chan 2025-07
        +stats 2025-07 (uses current channel)
        """
        try:
            current_channel = msg.args[0] if ircutils.isChannel(msg.args[0]) else None
            
            # Handle parameters
            if opt_channel and opt_year_month:
                # Both parameters provided
                target_channel = opt_channel
                year, month = map(int, opt_year_month.split('-'))
            elif opt_channel and '-' in opt_channel:
                # Only year_month provided (opt_channel parameter is actually year_month)
                target_channel = current_channel
                year, month = map(int, opt_channel.split('-'))
            elif opt_channel:
                # Only channel provided
                target_channel = opt_channel
                current_time = datetime.datetime.now()
                year, month = current_time.year, current_time.month
            else:
                # No parameters provided
                target_channel = current_channel
                current_time = datetime.datetime.now()
                year, month = current_time.year, current_time.month
            
            if not target_channel:
                irc.error("No channel specified and not in a channel.", private=True)
                return
            
            # Get data for the specified channel and time period
            channel_data = self.wordstats.get(target_channel, {})
            month_data = channel_data.get(year, {}).get(month, {})
            
            if not month_data:
                raise KeyError
                
            users = sorted(
                [(nick, counts['lines'], counts['words']) for nick, counts in month_data.items()],
                key=lambda x: (-x[2], -x[1]))
                
            # Set column widths
            pos_width = 4     # Width for position numbers
            nick_width = 15   # Increased width for usernames
            lines_width = 10   # Width for lines count
            words_width = 10   # Width for words count

            # Build the format strings with increased spacing
            header_format = "{:<4} | {:<15} | {:>10} | {:>10}"
            row_format = "{:<4} | {:<15} | {:>10} | {:>10}"

            table_header = header_format.format('Pos', 'Username', 'Lines', 'Words')
            
            # Prepare output
            header = f"Results for {target_channel} - {year}-{month:02d}"
            separator = '-' * len(table_header)

            output_messages = [
                header,
                separator,
                table_header,
                separator
            ]

            # Add data rows with new spacing
            for pos, (nick, line_count, word_count) in enumerate(users[:20], 1):
                output_messages.append(
                    row_format.format(
                        str(pos),
                        nick,
                        line_count,
                        word_count
                    )
                )

            # Add final separator
            output_messages.append(separator)

            # Send output
            for message in output_messages:
                irc.reply(message, private=True)
                
        except (ValueError, KeyError):
            irc.error("Invalid format or no data available. Use: +stats [channel] [YYYY-MM]", private=True)

    # Proper command wrapping with matching parameters
    stats = wrap(stats, [
        optional('channel'),
        optional('somethingWithoutSpaces')
    ])

    def die(self):
        """Save data when the plugin is unloaded."""
        self._save_stats()
        super().die()

Class = WordCounter