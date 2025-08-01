import sqlite3
import time
import sqlcipher3 as sqlcipher
import os
import threading
import random
import pytz
import supybot.world  # Import the supybot world
import supybot.ircmsgs as ircmsgs  # Import IRC message functions
from supybot.commands import *
from supybot import conf
from datetime import datetime, time as dt_time, timedelta
import supybot.callbacks as callbacks

class Serve(callbacks.Plugin):
    def __init__(self, irc):
        self.__parent = super(Serve, self)
        self.__parent.__init__(irc)
        self.passphrase = os.getenv("SQLITE_PASSPHRASE")
        if not self.passphrase:
            self.log.error("SQLITE_PASSPHRASE environment variable not set!")

        # Centralize database location using Supybot's directory configuration
        self.db_path = conf.supybot.directories.data.dirize("Serve/servestats.db")
        self.init_db()  # Ensure tables exist
        # Establish and initialize the database if needed persistently
        self.db = sqlcipher.connect(self.db_path)        

        # Dictionary to track the last command time for each user
        self.last_command_time = {}

        # Initialize and schedule daily reset
        self.timer = None
        self.schedule_daily_midnight_reset()       

        # Settings for spam replies and date format
        self.settings = {
            "antispam": (1, 3),  # 1 trigger every 3 seconds
            "spamreplies": [
                "Hey hey there, don't you think it's going a bit too fast there? Only {since} sec, since your last ...",
                "I am busy doing something else",
                "Haven't you just had ?",
                "remember to breath to",
                "please pay, before asking for more!",
            ],
            "dateformat": "%H:%M:%S",  # Corresponds to 'h:i:s' in PHP
        }

    def add_ordinal_number_suffix(self, num):
        if not (num % 100 in [11, 12, 13]):
            if num % 10 == 1:
                return f"{num}st"
            elif num % 10 == 2:
                return f"{num}nd"
            elif num % 10 == 3:
                return f"{num}rd"
        return f"{num}th"

    def init_db(self):
        """Initializes the SQLCipher database with proper schema and indexes."""
        if not self.passphrase:
            self.log.error("SQLITE_PASSPHRASE not set.")
            return
        try:
            with sqlcipher.connect(self.db_path) as db_conn:
                db_conn.execute(f"PRAGMA key = '{self.passphrase}';")
                db_conn.execute('''
                    CREATE TABLE IF NOT EXISTS servestats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nick TEXT,
                        address TEXT,
                        type TEXT,
                        last REAL,
                        today INTEGER,
                        total INTEGER,
                        channel TEXT,
                        network TEXT,
                        UNIQUE(nick, type, channel, network)
                    )''')
                db_conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_servestats_main 
                    ON servestats(nick, type, channel, network)
                ''')
                db_conn.commit()
        except Exception as e:
            self.log.error(f"Error initializing database: {str(e)}")           

    def schedule_daily_midnight_reset(self):
        try:
            local_tz = pytz.timezone('Europe/Copenhagen')  # Set your local timezone
            now = datetime.now(local_tz)
            midnight_time = local_tz.localize(datetime.combine(now.date(), dt_time(0, 0)))

            # Schedule for the next midnight if we're already past today's midnight
            if now >= midnight_time:
                midnight_time += timedelta(days=1)

            seconds_until_midnight = (midnight_time - now).total_seconds()
            self.log.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}, Next reset at: {midnight_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                          f"Seconds until reset: {seconds_until_midnight:.2f}")

            # Schedule the reset task
            self.timer = threading.Timer(seconds_until_midnight, self.reset_today_stats)
            self.timer.start()
        except Exception as e:
            self.log.error(f"Error occurred while scheduling daily reset: {str(e)}")            

    def reset_today_stats(self):
        """Initializes the SQLCipher database for storing release information."""
        passphrase = os.getenv("SQLITE_PASSPHRASE")
        if not passphrase:
            self.log.error("SQLITE_PASSPHRASE environment variable is not set.")
            return        
        try:
            self.log.info("Resetting 'today' stats.")
            # Open a new connection within the method
            with sqlcipher.connect(self.db_path) as db_conn:
                db_conn.execute(f"PRAGMA key = '{passphrase}';")
                db_conn.execute('''UPDATE servestats SET today = 0''')
                db_conn.commit()
            self.log.info("Stats reset successfully.")
            self.post_reset_message()
        except Exception as e:
            self.log.error(f"Error occurred while resetting 'today' stats: {str(e)}")
        finally:
            # Reschedule the reset for the next midnight
            self.schedule_daily_midnight_reset()

    def post_reset_message(self):
        try:
            # Create a message to post to the channel
            channel = "#bot"  # Specify the channel
            message = "Daily stats have been reset to 0!"

            # Log the message posting event
            self.log.info(f"Posting reset message to channel {channel}: {message}")

            # Iterate over all IRC networks
            for irc in supybot.world.ircs:  # Assuming ircs is a list of IRC objects
                if irc.network == "omg":  # Replace "network" with the correct attribute
                    # Queue the message to the channel
                    irc.queueMsg(ircmsgs.privmsg(channel, message))
                    break  # Exit loop once the message is sent to the desired network
        except Exception as e:
            self.log.error(f"Error occurred while resetting 'today' stats: {str(e)}")                

    def die(self):
        self.__parent.die()
        if self.timer is not None and self.timer.is_alive():
            self.timer.cancel()
            self.log.info("Scheduled timer cancelled.")
        self.log.info("Plugin shutdown and resources cleaned up.")      

    def _get_stats(self, nick, drink_type, channel, network):
        """Retrieves stats from the SQLCipher database."""
        passphrase = os.getenv("SQLITE_PASSPHRASE")
        if not passphrase:
            self.log.error("SQLITE_PASSPHRASE environment variable is not set.")
            return 0, 0  # Return default values if no passphrase

        try:
            today = int(time.strftime('%Y%m%d'))
            # Connect to the database using SQLCipher
            with sqlcipher.connect(self.db_path) as db_conn:
                db_conn.execute(f"PRAGMA key = '{passphrase}';")  # Set the encryption key
                cursor = db_conn.cursor()  # Make sure you're using the correct connection object

                # Fetch the sum of total and today
                cursor.execute('''SELECT total, today
                                FROM servestats
                                WHERE network = ? AND type = ? AND nick = ? AND channel = ?''',
                            (network, drink_type, nick, channel))

                result = cursor.fetchone()
                if result and all(value is not None for value in result):
                    total, today = result
                else:
                    total, today = 0, 0  # Set defaults if no data is found

                return today, total

        except Exception as e:
            self.log.error(f"Error occurred while resetting 'today' stats: {str(e)}")
            return 0, 0  # Return default values in case of error    


    def _update_stats(self, nick, address, drink_type, channel, network):
        """Updates stats using an efficient UPSERT operation."""
        if not self.passphrase:
            return 0, 0
        try:
            current_time = time.time()
            with sqlcipher.connect(self.db_path) as db_conn:
                db_conn.execute(f"PRAGMA key = '{self.passphrase}';")
                cursor = db_conn.cursor()
                # UPSERT to handle insert or update in one operation
                cursor.execute('''
                    INSERT INTO servestats (nick, address, type, last, today, total, channel, network)
                    VALUES (?, ?, ?, ?, 1, 1, ?, ?)
                    ON CONFLICT(nick, type, channel, network) DO UPDATE SET
                        today = today + 1,
                        total = total + 1,
                        last = excluded.last,
                        address = excluded.address
                ''', (nick, address, drink_type, current_time, channel, network))
                db_conn.commit()
                # Retrieve updated counts
                cursor.execute('''
                    SELECT today, total FROM servestats
                    WHERE nick=? AND type=? AND channel=? AND network=?
                ''', (nick, drink_type, channel, network))
                today_count, total_count = cursor.fetchone() or (1, 1)
                return today_count, total_count
        except Exception as e:
            self.log.error(f"Failed to update stats: {str(e)}")
            return 0, 0     

    def _select_spam_reply(self, last_served_time):
        """Select a spam reply if the user requests drinks too quickly."""
        time_since_last = time.time() - last_served_time
        # Convert the time since last command to seconds
        formatted_time = int(time_since_last)  # Get the time in seconds
        reply = random.choice(self.settings["spamreplies"])
        return reply.format(since=formatted_time)

    def _is_spamming(self, nick):
        """Check if the user is spamming commands too quickly."""
        current_time = time.time()
        last_time = self.last_command_time.get(nick, 0)

        # Get the antispam configuration
        _, seconds = self.settings["antispam"]  # We no longer need 'triggers'

        # Calculate the threshold in seconds
        threshold = seconds

        # Check the time difference based on the antispam settings
        if current_time - last_time < threshold:  # Allow one command every 'seconds'
            return True
        
        self.last_command_time[nick] = current_time  # Update the last command time
        return False

    def handle_spam(self, nick, irc):
        """Check if a user is spamming commands and reply if so."""
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return True  # Indicate that the user is spamming
        return False  # No spam detected        
# BAR MENU
    @wrap([optional('channel')])
    def bar(self, irc, msg, args, channel):
        """+bar - Show all available drink commands."""
        # List of available drink commands
        available_drinks = [
            "+anal",     # Anal
            "+lsd",      # LSD
            "+cola",     # Cola
            "+sprite",   # Sprite
            "+fanta",    # Fanta
            "+pepsi",    # Pepsi
            "+dew",      # Dew
            "+beer",     # Beer
            "+coffee",   # Coffee
            "+redbull",  # Redbull
            "+tea",      # Tea
            "+cap",      # Cappuccino
            "+whiskey",  # Whiskey
            "+wine",     # Wine
            "+ice",      # Ice cream              
            "+gumbo",    # Gumbo
            "+ganja",    # Ganja
            "+mix",      # Mix
            "+head",     # Head
            "+pipe",     # Pipe
            "+coke",     # Coke
            "+pussy",    # Pussy
            "+surprise", # Surprise
            "+tequila"   # tequila   
            # Add other drink commands here as needed
        ]

        # Format the response
        response = "Available on the menu: " + ", ".join(available_drinks)
        
        # Send the response
        irc.reply(response)    
# END

# CUSTOM STUFF
    @wrap([optional('channel')])
    def anal(self, irc, msg, args, channel):
        """+anal - Gives anal."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the nick is "ValFar"
        if nick != "ValFar":
            irc.reply("You are not cool enough to use this command", prefixNick=True)
            return  # Do nothing if the nickname is not "ValFar"

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming        

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "anal", channel, network)

        responses = [
            f"now, {nick}, bend over, and you'll feel a little poke in about 3 seconds ({today_count}/{total_count})"
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

# Ganja
    @wrap([optional('channel')])
    def ganja(self, irc, msg, args, channel):
        """+ganja - hands some ganja."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming      

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "ganja", channel, network)

        # Add ordinal suffix to total_count
        responses = [
            f"serves ganja-infused tea to {nick} ({today_count}/{total_count})",
            f"serves a dry ganja leaf wrapped in a newspaper to {nick} ({today_count}/{total_count})",
            f"serves ganja brownies fresh from the oven to {nick} ({today_count}/{total_count})",
            f"serves a warm ganja milkshake that's slightly questionable to {nick} ({today_count}/{total_count})",
            f"serves a perfectly rolled ganja joint, chilled to perfection in the fridge to {nick} ({today_count}/{total_count})",
            f"serves ganja gummies that have been left out in the sun, slightly sticky, to {nick} ({today_count}/{total_count})",
            f"serves a ganja-laced cola that no one expected but everyone loves to {nick} ({today_count}/{total_count})",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)        
# END

# Ganja
    @wrap([optional('channel')])
    def lsd(self, irc, msg, args, channel):
        """+lsd - hands some ganja."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming      

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "lsd", channel, network)

        # Add ordinal suffix to total_count
        responses = [
            f"serves a colorful LSD-infused tea to {nick} ({today_count}/{total_count})",
            f"serves a blotter of LSD artfully wrapped in a vintage newspaper to {nick} ({today_count}/{total_count})",
            f"serves freshly baked brownies with a subtle hint of LSD to {nick} ({today_count}/{total_count})",
            f"serves a warm LSD milkshake with a swirl of mystery to {nick} ({today_count}/{total_count})",
            f"serves a perfectly dosed LSD tab, chilled and ready for an adventure, to {nick} ({today_count}/{total_count})",
            f"serves sticky LSD gummy bears left out in the sun, perfect for a trip, to {nick} ({today_count}/{total_count})",
            f"serves a surprising LSD-laced cola that makes every sip a journey to {nick} ({today_count}/{total_count})",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)        
# END       

# DRINKS
    @wrap([optional('something'), optional('channel')])
    def cola(self, irc, msg, args, nickname, channel):
        """+cola [nickname] - Serve some cola. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network
   
        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "cola", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "serves ice-cold cola to {to_nick} ({today_count}/{total_count})",
            "serves Coca Cola that has been laying in a pile of shit, ~45°C to {to_nick} ({today_count}/{total_count})",
            "serves Coca Cola that's been standing close to a box of dry ice, ~1.3°C to {to_nick} ({today_count}/{total_count})",
            "serves a warm, flat Coca Cola that no one wants ~25°C to {to_nick} ({today_count}/{total_count})",
            "serves Coca Cola fresh from the fridge, chilled to perfection ~5°C to {to_nick} ({today_count}/{total_count})",
            "serves Coca Cola that tastes slightly metallic after standing in a can too long, ~18°C to {to_nick} ({today_count}/{total_count})",
            "serves Coca Cola Zero fresh from the fridge, chilled to perfection ~5°C to {to_nick} ({today_count}/{total_count})"
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"serves a nice cold Coca Cola ({today_count}/{total_count})",
            f"serves a nice cold Coca Cola Zero ({today_count}/{total_count})",
            f"serves a nice cold Coca Cherry ({today_count}/{total_count})",
            f"serves a nice cold Coca Cola Vanilla ({today_count}/{total_count})",
            f"serves a nice cold Coca Cola Vanilla Zero ({today_count}/{total_count})",
            f"want cola? I'm serving sarcasm on tap today. ({today_count}/{total_count})",
            f"you're thirsty? Too bad, I'm all out of care today ({today_count}/{total_count})",
            f"oh, you want a cola? How about you try refreshing your life choices first? ({today_count}/{total_count})",
            f"cola? Coming right up! It's 99% virtual and 1% imagination! ({today_count}/{total_count})",
            f"one cola, straight from my nonexistent fridge. Enjoy! {nick} ({today_count}/{total_count})",
            f"if only you could actually drink pixels... ({today_count}/{total_count})",
            f"here's your cola virtually hands you an empty can—oops, guess I drank it! {nick} ({today_count}/{total_count})",
            f"cola? On the house! Well, technically in your head. {nick} ({today_count}/{total_count})",
            f"one virtual cola, served with a side of 'don't spill it on your keyboard! ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response)

    @wrap([optional('channel')])
    def fanta(self, irc, msg, args, channel):
        """+fanta - Serve a Fanta."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "fanta", channel, network)

        responses = [
            f"hands a refreshing Fanta Lemon, chilled to a perfect ~5°C, to {nick} – enjoy! ({today_count}/{total_count})",
            f"offers an icy-cold Fanta Orange, straight from the fridge at ~5°C, to {nick} – cheers! ({today_count}/{total_count})",
            f"serves up a vibrant Fanta Mango, cooled just right at ~5°C, to {nick} – take a sip! ({today_count}/{total_count})",
            f"delivers a crisp Fanta Lemon Zero, frosty and refreshing at ~5°C, to {nick} – bottoms up! ({today_count}/{total_count})",
            f"slides over a perfectly chilled Fanta Orange Zero, kept at ~5°C, to {nick} – savor it! ({today_count}/{total_count})",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)         

    @wrap([optional('channel')])
    def pepsi(self, irc, msg, args, channel):
        """+pepsi - Serve a Pepsi."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "pepsi", channel, network)

        responses = [
            f"hands over a crisp and cool Pepsi Peach, straight from ~5°C, to {nick} – enjoy the burst of flavor! ({today_count}/{total_count})"
            f"slides a frosty Pepsi Peach, perfectly chilled at ~5°C, to {nick} – drink up and relax! ({today_count}/{total_count})",
            f"serves a refreshing Pepsi Peach, frosty at ~5°C, to {nick} – savor the moment! ({today_count}/{total_count})",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

    @wrap([optional('channel')])
    def dew(self, irc, msg, args, channel):
        """+dew - Serve a dew."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "dew", channel, network)

        responses = [
            f"hands over a crisp and cool Mountain Dew, straight from ~5°C, to {nick} – enjoy the burst of flavor! ({today_count}/{total_count})"
            f"slides a frosty Diet Mountain Dew, perfectly chilled at ~5°C, to {nick} – drink up and relax! ({today_count}/{total_count})",
            f"serves a refreshing Mountain Dew Code Red, frosty at ~5°C, to {nick} – savor the moment! ({today_count}/{total_count})",
            f"hands over a crisp and cool Mountain Dew LiveWire, chilled to a perfect ~5°C, to {nick} – enjoy the zesty burst of citrus! ({today_count}/{total_count})"
            f"slides a frosty Mountain Dew Baja Blast, fresh from ~5°C, to {nick} – let the tropical vibes take over! ({today_count}/{total_count})"
            f"serves a vibrant Mountain Dew Voltage, icy at ~5°C, to {nick} – feel the electrifying charge of raspberry citrus! ({today_count}/{total_count})"
            f"delivers a classic Mountain Dew Real Sugar, perfectly chilled at ~5°C, to {nick} – indulge in the timeless original! ({today_count}/{total_count})"
            f"offers a rare Mountain Dew Goji Citrus Strawberry, frosted at ~5°C, to {nick} – experience the unique blend of exotic flavors! ({today_count}/{total_count})"
            f"hands a mysterious Mountain Dew Voo-Dew, straight from ~5°C, to {nick} – uncover the spooky surprise of flavor! ({today_count}/{total_count})"
            f"slides a juicy Mountain Dew Major Melon, cooled to ~5°C, to {nick} – take a sip of that watermelon sweetness! ({today_count}/{total_count})"
            f"serves a tangy Mountain Dew Spark, ice-cold at ~5°C, to {nick} – enjoy the bold zing of raspberry lemonade! ({today_count}/{total_count})"            
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)         

    @wrap([optional('channel')])
    def sprite(self, irc, msg, args, channel):
        """+sprite - Serve some Sprite"""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming            

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "sprite", channel, network)

        responses = [
            f"serves ice-cold Sprite to {nick} ({today_count}/{total_count})",
            f"serves ice-cold Sprite Zero to {nick} ({today_count}/{total_count})",
            f"serves a semi cold Sprite to {nick} ({today_count}/{total_count})",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)        

    @wrap([optional('something'), optional('channel')])
    def beer(self, irc, msg, args, nickname, channel):
        """+beer [nickname] - Serve some beers. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming      

        # Update stats
        today_count, total_count= self._update_stats(nick, address, "beer", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "serves ice-cold beer to {to_nick} ({today_count}/{total_count})",
            "serves a beer that has been laying in a pile of shit. It's 45°C, to {to_nick} ({today_count}/{total_count})",
            "serves a warm beer to {to_nick} ({today_count}/{total_count})",
            "Here's your beer! Don't drink it all in one sip... wait, never mind, it's {to_nick}, go ahead! ({today_count}/{total_count})",

        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"slides a beer over Remember, it's beer, not life advice! ({today_count}/{total_count})",
            f"one beer coming right up! Just promise not to embarrass yourself... again. ({today_count}/{total_count})",
            f"gives you a beer Now you're only half as annoying! ({today_count}/{total_count})",
            f"a beer for you! Because that's cheaper than therapy. ({today_count}/{total_count})",
            f"here's your beer! Because, apparently, this is what I get paid to do... oh wait, I don't get paid. ({today_count}/{total_count})",
            f"Hands you a beer Careful, it's so light you might float away! ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response)          

    @wrap([optional('channel')])
    def coffee(self, irc, msg, args, channel):
        """+coffee - Make a coffee."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming            

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "coffee", channel, network)

        # Add ordinal suffix to total_count
        ordinal_suffix = self.add_ordinal_number_suffix(total_count)

        responses = [
            f"making a cup of coffee for {nick} 🍵. {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made coffee for you.",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

    @wrap([optional('channel')])
    def redbull(self, irc, msg, args, channel):
        """+redbull - Serve a redbull."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "redbull", channel, network)

        # Add ordinal suffix to total_count
        ordinal_suffix = self.add_ordinal_number_suffix(total_count)

        responses = [
            f"Grabs another cold Redbull for {nick}. That's {today_count} times today, bringing your total count to {total_count}. This marks the {ordinal_suffix} time overall!",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)        

    @wrap([optional('channel')])
    def tea(self, irc, msg, args, channel):
        """+tea - Make a cup of tea."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming          

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "tea", channel, network)

        # Add ordinal suffix to sumtotal
        ordinal_suffix = self.add_ordinal_number_suffix(total_count)

        responses = [
            f"A cup of tea is on the way to {nick}! 🍵 Time to relax and enjoy the moment, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
            f"One piping hot cup of tea coming right up {nick}! ☕ Don't spill the tea, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.", 
            f"Ah, splendid choice {nick}! 🫖 Your Earl Grey shall be served with a side of elegance, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
            f"Your tea is ready {nick}! 🍵 Let it soothe your soul and calm your mind, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
            f"A cup of David's Tea Red Velvet, coming right up {nick}! 🍵 {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
            f"A cup of forest berries tea, coming right up, {nick} ☕ Enjoy it! {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I've made tea.",
            f"A cup of Spanish orange tea, coming right up, {nick} ☕ Enjoy it! {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I've made tea.",
            f"A cup of maple black tea, coming right up, {nick} ☕ Enjoy it! {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I've made tea.",
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

    @wrap([optional('channel')])
    def cap(self, irc, msg, args, channel):
        """+cap - Make a Cappuccino."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming           

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "cap", channel, network)

        # Add ordinal suffix to sumtotal
        ordinal_suffix = self.add_ordinal_number_suffix(total_count)

        responses = [
            f"Making a nice Cappuccino for {nick} 🍵. {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made Cappuccino."
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

    @wrap([optional('something'), optional('channel')])
    def whiskey(self, irc, msg, args, nickname, channel):
        """+wine [nickname] - Serve some wine. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming        

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "whiskey", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "serves whiskey chilled on the rocks to {to_nick}({today_count}/{total_count})",
            "found some weird-looking bottle in corner; might hit gold. Cheers {to_nick} ({today_count}/{total_count})",
            "You are in need of cola and bad whiskey, {to_nick} ({today_count}/{total_count})",
            "another whiskey? How about water this time, {to_nick}?",

        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"one whiskey coming right up! Drink responsibly... because typing sober didn't work out so well last time. ({today_count}/{total_count})",
            f"here's your whiskey. Now you can drink to forget... how bad your last joke was ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response) 

    @wrap([optional('something'), optional('channel')])
    def wine(self, irc, msg, args, nickname, channel):
        """+wine [nickname] - Serve some wine. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming        

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "wine", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "pours out a variety of good things from the basement {to_nick}({today_count}/{total_count})",
            "here you are {to_nick}; I found something out back. ({today_count}/{total_count})",
            "lucky you {to_nick}, we just have one of these left. Enjoy! ({today_count})",
            "so you're hit hard. Where you want it?, don't cry, {to_nick}({today_count}/{total_count})",

        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"wine! For when coffee isn't strong enough and tequila's a bit too honest. ({today_count}/{total_count})",
            f"wine incoming! Also known as 'Mommy's Little Helper'. ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response)

    @wrap([optional('something'), optional('channel')])
    def ice(self, irc, msg, args, nickname, channel):
        """+ice [nickname] - Serve a cola. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming        

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "ice", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "here {to_nick}... Only one ball is available for you ({today_count}/{total_count})",
            "finds a big ice cream for {to_nick} to eat and you get it for free ($50 to use the toilet). ({today_count}/{total_count})",
            "oh {to_nick}, you think you're cool? That's cute. Stay frozen, snowflake. ({today_count}/{total_count})",
            "you're about as chill as a toaster {to_nick} on fire. ({today_count}/{total_count})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            "dusts off something that looks like ice cream from the corner of the fridge. here you go, {nick} ({today_count}/{total_count})",
            "nice try. You're about as frosty as a wet sock. ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response)
# EATING     

    @wrap([optional('channel')])
    def gumbo(self, irc, msg, args, channel):
        """+coffee - Make a coffee."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming      

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "gumbo", channel, network)

        # Add ordinal suffix to total_count
        responses = [
            f"have a spicy bowl of gumbo from the bayou, {nick}. ({today_count}/{total_count})",
            f"enjoy my special gumbo dish, {nick}. ({today_count}/{total_count})",
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)
# END

# MIX/HASH/ETC
    @wrap([optional('channel')])
    def mix(self, irc, msg, args, channel):
        """+mix - You need something to get high with."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming            

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "mix", channel, network)

        # Fetch the list of users in the channel as a list
        channel_users = list(irc.state.channels[channel].users) if channel in irc.state.channels else []

        # Define the list of nicknames to exclude
        excluded_nicks = ['klapvogn', 'chat']  # Add any additional nicknames here

        # Exclude specified nicknames from the list of users
        potential_names = [user for user in channel_users if user not in excluded_nicks]

        # Select a random name from the channel users
        if potential_names:
            random_name = random.choice(potential_names)
        else:
            random_name = 'your mom'  # Fallback if no other users are present

        responses = [
            f"preparing a mixture by grinding up some weed ({today_count}/{total_count})",
            f"grabs some of the good stuff for a mix ({today_count}/{total_count})",
            f"sneaks into {random_name}'s stash and steals for a mix. Here you go. ({today_count}/{total_count})",
            f"go to India and hunt for strains that are good for your mix. ({today_count}/{total_count})",
            f"try strain hunting in Morocco to find some good stuff for your mix. ({today_count}/{total_count})",
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

    @wrap([optional('something'), optional('channel')])
    def pipe(self, irc, msg, args, nickname, channel):
        """+pipe [nickname] - Serve a cola. Optionally to a specific nickname."""
        
        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming  

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "pipe", channel, network)

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "go strain hunting in Morocco for some good stuff for your pipe, {to_nick}({today_count}/{total_count})",
            "upon seeing some trash in the corner, I filled a pipe, {to_nick}({today_count}/{total_count})",
            "skunky just arrieved. peace all over, {to_nick}({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if not nickname:
            to_nick = ""  # No nickname, just empty string
        else:
            to_nick = f"to {nick} "  # If nickname is provided, add "to <nick>"

        # Select a random response and format it with stats and nickname info
        response = random.choice(responses).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        
        # Reply with the final formatted response
        irc.reply(response) 

    @wrap([optional('channel')])
    def coke(self, irc, msg, args, channel):
        """+coke - you need some coke"""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming           

        # Update stats
        today_count, total_count= self._update_stats(nick, address, "coke", channel, network)

        responses = [
            f"Are you stupid? We don't do shit like this... ^_^ ({total_count}/{today_count})"

        ]        

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)

    @wrap([optional('channel')])
    def head(self, irc, msg, args, channel):
        """+coke - you need some coke"""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming           

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "head", channel, network)

        responses = [
            f".h.e.a.d. ({total_count}/{today_count})",
            f"head for you sir. ({total_count}/{today_count})",
        ]        

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)             
# END

    @wrap([optional('something'), optional('channel')])
    def pussy(self, irc, msg, args, nickname, channel):
        """+pussy [nickname] - Serve a some pussy. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "pussy", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "slaps {to_nick} in face with a smelly pussy ({today_count}/{total_count})"
            "Sends some pussy {to_nick}'s way.. ({today_count}/{total_count})",
            "if you have the cash, {to_nick}, I can pull down my undies for you ^_^ ({today_count}/{total_count})",
            "follow me, {to_nick}, I have something I want to show you ^_^ ({today_count}/{total_count})",
            "wait here, {to_nick}, I'll be back with some supreme pussy for you ({today_count}/{total_count})",
            "for that amount of money, {to_nick}, I can only show you my tits ({today_count}/{total_count})",
            "ohh big spender, {to_nick}, here you have me fully undressed ({today_count}/{total_count})",
            "play nice, {to_nick}, and maybe I'll go down on my knees for you ({today_count}/{total_count})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"free pussy to all. When you find the key to my chastity belt. ^_^ ({today_count}/{total_count})",
            f"not enough money to supply you as well... ({today_count}/{total_count})",
            f"Nice try, but I don't think your IQ can handle that kind of complexity ({today_count}/{total_count})",
            f"Did your brain take a vacation when you typed that? ({today_count}/{total_count})",
            f"Did someone say 'purr-fect'? Because I'm all ears! ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response) 
        
    @wrap([optional('something'), optional('channel')])
    def surprise(self, irc, msg, args, nickname, channel):
        """+surprise [nickname] - Serve a nice surprise. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming        

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "surprise", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "makes a sandwich for {to_nick}. enjoy this supreme dish ({today_count}/{total_count})",
            "pours a cup of fresh cow milk to {to_nick} ({today_count}/{total_count})",
            "goes out back and chopping the head off a chicken, so a great grilled chicken can be made for {to_nick}({today_count}/{total_count})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            "GET OUT OF MY BAR! ({today_count}/{total_count})",
            "I only serve to people over 18+ ({today_count}/{total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count)

        # Reply with the final formatted response
        irc.reply(response)

    @wrap([optional('channel')])
    def tequila(self, irc, msg, args, channel):
        """+tequila - Serve a tequila."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming
        if self.handle_spam(nick, irc):
            return  # Stop processing if spamming       

        # Update stats
        today_count, total_count = self._update_stats(nick, address, "tequila", channel, network)

        responses = [
            f"hands over a crisp and smooth shot of silver tequila to {nick} – enjoy the bright, zesty kick! ({today_count}/{total_count})"
            f"slides a frosty glass of tequila sunrise to {nick} – savor the perfect mix of sweet and tangy! ({today_count}/{total_count})"
            f"serves a refreshing margarita, with a splash of tequila, to {nick} – take a sip and unwind! ({today_count}/{total_count})"
            f"hands over a bold reposado tequila to {nick} – enjoy the smooth notes of oak and vanilla! ({today_count}/{total_count})"
            f"slides a tropical tequila punch to {nick} – let the island vibes take over! ({today_count}/{total_count})"
            f"serves a vibrant paloma, spiked with tequila, to {nick} – feel the citrusy burst of grapefruit and lime! ({today_count}/{total_count})"
            f"delivers a classic tequila shot with lime and salt to {nick} – indulge in the timeless ritual! ({today_count}/{total_count})"
            f"offers a rare añejo tequila to {nick} – experience the rich blend of aged flavors! ({today_count}/{total_count})"
            f"hands a mysterious tequila mezcal cocktail to {nick} – uncover the smoky surprise in every sip! ({today_count}/{total_count})"
            f"slides a juicy watermelon tequila cooler to {nick} – take a refreshing sip of fruity sweetness! ({today_count}/{total_count})"
            f"serves a tangy tequila smash to {nick} – enjoy the bold fusion of herbs and citrus! ({today_count}/{total_count})"
       
            # Additional response variations can be added here
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)               

Class = Serve