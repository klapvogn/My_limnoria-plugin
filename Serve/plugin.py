import sqlite3
import time
import threading
import random
import pytz
import supybot.ircmsgs as ircmsgs
from supybot.commands import *
from datetime import datetime, time as dt_time, timedelta
import supybot.callbacks as callbacks

class Serve(callbacks.Plugin):
    def __init__(self, irc):
        self.__parent = super(Serve, self)
        self.__parent.__init__(irc)
        self.__parent.die()
        self.db = sqlite3.connect("/home/ubuntu/limnoria/plugins/Serve/servestats.db")
        self.init_db()
        # Dictionary to track the last command time for each user
        self.last_command_time = {}
        # Schedule the midnight reset task
        self.timer = None  # Store the timer so it can be canceled later
        self.schedule_daily_midnight_reset()            

        # Settings for spam replies and date format
        self.settings = {
            "antispam": (1, 3),  # 1 trigger every 3 seconds
            "spamreplies": [
                "Hey hey there, don't you think it's going a bit too fast there? Only {since} sec, since your last ...",
                "I am busy doing something else",
                "Haven't you just had ?",
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
        # Create the table if it doesn't exist
        with self.db:
            self.db.execute('''CREATE TABLE IF NOT EXISTS servestats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nick TEXT NOT NULL,
                address TEXT NOT NULL,
                type TEXT NOT NULL,
                last REAL NOT NULL,
                today INTEGER NOT NULL,
                total INTEGER NOT NULL,
                channel TEXT NOT NULL,
                network TEXT NOT NULL
            )''')

    def schedule_daily_midnight_reset(self):
        # Set your local timezone (e.g., Europe/Copenhagen)
        local_tz = pytz.timezone('Europe/Copenhagen')  # Change this to your local timezone

        # Get the current time in the correct timezone
        now = datetime.now(local_tz)

        # Set today's date and specific time to midnight
        today = now.date()
        midnight_time = local_tz.localize(datetime.combine(today, dt_time(0, 0)))

        # Check if it's already past midnight
        if now >= midnight_time:
            # If so, schedule for the next midnight
            midnight_time += timedelta(days=1)

        self.log.info(f"Next scheduled reset at: {midnight_time}")

        # Calculate the seconds until the next midnight
        seconds_until_midnight = (midnight_time - now).total_seconds()

        # Log current time and scheduling details
        formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
        formatted_midnight_time = midnight_time.strftime("%Y-%m-%d %H:%M:%S")
        
        self.log.info(f"Current time: {formatted_now}, Next midnight: {formatted_midnight_time}, "
                    f"Seconds until next midnight: {seconds_until_midnight:.2f}")

        # Schedule the task to run at the next midnight
        threading.Timer(seconds_until_midnight, self.reset_today_stats).start()

    def reset_today_stats(self):
        if not hasattr(self, 'irc'):
            self.log.error("The 'irc' attribute is not initialized.")
            return        
        try:
            # Log the attempt to reset stats
            self.log.info("Attempting to reset 'today' stats.")

            # Create a new database connection for this thread
            with sqlite3.connect('/home/ubuntu/limnoria/plugins/Serve/servestats.db') as db_conn:
                cursor = db_conn.cursor()

                # Log the query being executed
                self.log.info("Executing SQL query to reset 'today' stats.")
                cursor.execute('''UPDATE servestats SET today = 0''')

                # Commit the changes
                db_conn.commit()

            # Log that the reset is complete
            self.log.info("Stats reset complete.")

            # Call the post_reset_message function to send the message
            self.post_reset_message()            
        
        except Exception as e:
            # Log any error that occurs during the reset
            self.log.error(f"Error occurred while resetting 'today' stats: {str(e)}")

    def post_reset_message(self):
        # Create a message to post to the channel
        channel = "#bot"  # Specify the channel
        message = "Daily stats have been reset to 0!"
        
        # Log the message posting event
        self.log.info(f"Posting reset message to channel {channel}: {message}")

        # Get the IRC object for the specified network
        irc = supybot.world.ircs["omg"]  # Replace "networkname" with the actual network name

        # Queue the message to the channel
        irc.queueMsg(ircmsgs.privmsg(channel, message))

    def die(self):
        # Stop the timer if it's running
        if self.timer is not None:
            self.timer.cancel()
            self.log.info("Scheduled timer cancelled.")

        # Perform any other necessary cleanup here
        # E.g., closing database connections or other resources
        self.log.info("Plugin is shutting down and cleaning up resources.")        

    def _get_stats(self, nick, drink_type, channel, network): 
        today = int(time.strftime('%Y%m%d'))
        cursor = self.db.cursor()

        # Fetch the sum of total and today
        cursor.execute('''SELECT SUM(total) AS sumtotal, SUM(today) AS sumtoday
                        FROM servestats
                        WHERE network = ? AND type = ? AND nick = ?''',  # Added nick to filter properly
                    (network, drink_type, nick))

        result = cursor.fetchone()
        if result and all(value is not None for value in result):
            sumtotal, sumtoday = result
        else:
            sumtotal, sumtoday = 0, 0  # Set defaults if no data is found

        return sumtoday, sumtotal

    def _update_stats(self, nick, address, drink_type, channel, network):
        today = int(time.strftime('%Y%m%d'))
        cursor = self.db.cursor()

        # Call _get_stats to retrieve the current counts
        today_count, total_count = self._get_stats(nick, drink_type, channel, network)

        # Check if total_count is 0 to determine if it's the first entry
        if total_count == 0:
            today_count = 1  # First entry for today
            total_count = 1  # Total count also starts at 1
            cursor.execute('''INSERT INTO servestats (nick, address, type, last, today, total, channel, network)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (nick, address, drink_type, time.time(), today_count, total_count, channel, network))
        else:
            # Increment the counts in the existing entry
            cursor.execute('''UPDATE servestats
                            SET today = today + 1, total = total + 1, last = ?
                            WHERE nick = ? AND type = ? AND channel = ? AND network = ?''',
                        (time.time(), nick, drink_type, channel, network))
            
            today_count += 1  # Increment today's count in the code
            total_count += 1  # Increment total count in the code

        self.db.commit()  # Commit the changes

        print(f"Updated stats - Today: {today_count}, Total: {total_count} for {nick}")  # Debug output
        return today_count, total_count  # Return updated counts

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
        triggers, seconds = self.settings["antispam"]

        # Calculate the threshold in seconds
        threshold = seconds

        # Check the time difference based on the antispam settings
        if current_time - last_time < threshold:  # Allow one command every 'seconds'
            return True
        
        self.last_command_time[nick] = current_time  # Update the last command time
        return False
# BAR MENU
    @wrap([optional('channel')])
    def bar(self, irc, msg, args, channel):
        """+bar - Show all available drink commands."""
        # List of available drink commands
        available_drinks = [
            "+cola",     # Cola
            "+beer",     # Beer
            "+coffee",   # Coffee
            "+tea",      # Tea
            "+cap",      # Cappuccino
            "+whiskey",  # Whiskey
            "+wine",     # Wine
            "+ice",      # Ice cream            
            "+mix",      # Mix
            "+head",     # Head
            "+pipe",     # Pipe
            "+coke",     # Coke
            "+pussy",    # Pussy
            "+surprise"  # Surprise
            # Add other drink commands here as needed
        ]

        # Format the response
        response = "Available on the menu: " + ", ".join(available_drinks)
        
        # Send the response
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

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "cola", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "serves ice-cold cola to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves cola that has been laying in a pile of shit, ~45°C to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves cola that's been standing close to a box of dry ice, ~1.3°C to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves a warm, flat cola that no one wants ~25°C to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves cola fresh from the fridge, chilled to perfection ~5°C to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves cola that tastes slightly metallic after standing in a can too long, ~18°C to {to_nick} ({today_count}/{total_count}/{sumtotal})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"want cola? I'm serving sarcasm on tap today. ({today_count}/{total_count}/{sumtotal})",
            f"you're thirsty? Too bad, I'm all out of care today ({today_count}/{total_count}/{sumtotal})",
            f"oh, you want a cola? How about you try refreshing your life choices first? ({today_count}/{total_count}/{sumtotal})",
            f"cola? Coming right up! It's 99% virtual and 1% imagination! ({today_count}/{total_count}/{sumtotal})",
            f"one cola, straight from my nonexistent fridge. Enjoy! {nick} ({today_count}/{total_count}/{sumtotal})",
            f"if only you could actually drink pixels... ({today_count}/{total_count}/{sumtotal})",
            f"here's your cola virtually hands you an empty can—oops, guess I drank it! {nick} ({today_count}/{total_count}/{sumtotal})",
            f"cola? On the house! Well, technically in your head. {nick} ({today_count}/{total_count}/{sumtotal})",
            f"one virtual cola, served with a side of 'don't spill it on your keyboard! ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response)

    @wrap([optional('something'), optional('channel')])
    def beer(self, irc, msg, args, nickname, channel):
        """+beer [nickname] - Serve some beers. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "beer", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "serves ice-cold beer to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves a beer that has been laying in a pile of shit. It's 45°C, to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "serves a warm beer to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "Here's your beer! Don't drink it all in one sip... wait, never mind, it's {to_nick}, go ahead! ({today_count}/{total_count}/{sumtotal})",

        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"slides a beer over Remember, it's beer, not life advice! ({today_count}/{total_count}/{sumtotal})",
            f"one beer coming right up! Just promise not to embarrass yourself... again. ({today_count}/{total_count}/{sumtotal})",
            f"gives you a beer Now you're only half as annoying! ({today_count}/{total_count}/{sumtotal})",
            f"a beer for you! Because that's cheaper than therapy. ({today_count}/{total_count}/{sumtotal})",
            f"here's your beer! Because, apparently, this is what I get paid to do... oh wait, I don't get paid. ({today_count}/{total_count}/{sumtotal})",
            f"Hands you a beer Careful, it's so light you might float away! ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response)  

    @wrap([optional('channel')])
    def coffee(self, irc, msg, args, channel):
        """+coffee - Make a coffee."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return           

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
    def cap(self, irc, msg, args, channel):
        """+cap - Make a Cappuccino."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return            

        # Update stats
        #today_count, total_count, sumtotal = self._update_stats(nick, address, "cap", channel, network)
        today_count, total_count = self._update_stats(nick, address, "cap", channel, network)

        # Add ordinal suffix to sumtotal
        #ordinal_suffix = self.add_ordinal_number_suffix(sumtotal)
        ordinal_suffix = self.add_ordinal_number_suffix(total_count)

        responses = [
            f"Making a nice Cappuccino for {nick} 🍵. {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made Cappuccino for you."
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

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return           

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "tea", channel, network)

        # Add ordinal suffix to sumtotal
        ordinal_suffix = self.add_ordinal_number_suffix(sumtotal)

        responses = [
            f"A cup of tea is on the way to {nick}! 🍵 Time to relax and enjoy the moment, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
            f"One piping hot cup of tea coming right up {nick}! ☕ Don't spill the tea, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.", 
            f"Ah, splendid choice {nick}! 🫖 Your Earl Grey shall be served with a side of elegance, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
            f"Your tea is ready {nick}! 🍵 Let it soothe your soul and calm your mind, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made tea.",
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

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "whiskey", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "serves whiskey chilled on the rocks to {to_nick}({today_count}/{total_count}/{sumtotal})",
            "found some weird-looking bottle in corner; might hit gold. Cheers {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "You are in need of cola and bad whiskey, {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "another whiskey? How about water this time, {to_nick}?",

        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"one whiskey coming right up! Drink responsibly... because typing sober didn't work out so well last time. ({today_count}/{total_count}/{sumtotal})",
            f"here's your whiskey. Now you can drink to forget... how bad your last joke was ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response)  

    @wrap([optional('something'), optional('channel')])
    def wine(self, irc, msg, args, nickname, channel):
        """+wine [nickname] - Serve some wine. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "wine", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "pours out a variety of good things from the basement {to_nick}({today_count}/{total_count}/{sumtotal})",
            "here you are {to_nick}; I found something out back. ({today_count}/{total_count}/{sumtotal})",
            "lucky you {to_nick}, we just have one of these left. Enjoy! ({today_count}/{total_count}/{sumtotal})",
            "so you're hit hard. Where you want it?, don't cry, {to_nick}({today_count}/{total_count}/{sumtotal})",

        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"wine! For when coffee isn't strong enough and tequila's a bit too honest. ({today_count}/{total_count}/{sumtotal})",
            f"wine incoming! Also known as 'Mommy's Little Helper'. ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response)
# END

# EATING
    @wrap([optional('something'), optional('channel')])
    def ice(self, irc, msg, args, nickname, channel):
        """+ice [nickname] - Serve a cola. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "ice", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "here {to_nick}... Only one ball is available for you ({today_count}/{total_count}/{sumtotal})",
            "finds a big ice cream for {to_nick} to eat and you get it for free ($50 to use the toilet). ({today_count}/{total_count}/{sumtotal})",
            "oh {to_nick}, you think you're cool? That's cute. Stay frozen, snowflake. ({today_count}/{total_count}/{sumtotal})",
            "you're about as chill as a toaster {to_nick} on fire. ({today_count}/{total_count}/{sumtotal})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            "dusts off something that looks like ice cream from the corner of the fridge. here you go, {nick} ({today_count}/{total_count}/{sumtotal})",
            "nice try. You're about as frosty as a wet sock. ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response)
# END        

# MIX/HASH/ETC
    @wrap([optional('channel')])
    def mix(self, irc, msg, args, channel):
        """+mix - You need something to get high with."""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return            

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "mix", channel, network)

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
            f"preparing a mixture by grinding up some weed ({total_count})",
            f"grabs some of the good stuff for a mix ({total_count})",
            f"sneaks into {random_name}'s stash and steals for a mix. Here you go. ({total_count})",
            f"go to India and hunt for strains that are good for your mix. ({total_count})",
            f"try strain hunting in Morocco to find some good stuff for your mix. ({total_count})",
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

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "pipe", channel, network)

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "go strain hunting in Morocco for some good stuff for your pipe, {to_nick}({today_count}/{total_count}/{sumtotal})",
            "upon seeing some trash in the corner, I filled a pipe, {to_nick}({today_count}/{total_count}/{sumtotal})",
            "skunky just arrieved. peace all over, {to_nick}({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if not nickname:
            to_nick = ""  # No nickname, just empty string
        else:
            to_nick = f"to {nick} "  # If nickname is provided, add "to <nick>"

        # Select a random response and format it with stats and nickname info
        response = random.choice(responses).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        
        # Reply with the final formatted response
        irc.reply(response) 

    @wrap([optional('channel')])
    def coke(self, irc, msg, args, channel):
        """+coke - you need some coke"""
        nick = msg.nick
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return            

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "coke", channel, network)

        responses = [
            f"Are you stupid? We don't do shit like this... ^_^ ({total_count})"

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

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return            

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "head", channel, network)

        responses = [
            f".h.e.a.d. ({total_count})",
            f"head for you sir. ({total_count})",
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

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "pussy", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "slaps {to_nick} in face with a smelly pussy ({total_count})"
            "Sends some pussy {to_nick}'s way.. ({total_count})",
            "if you have the cash, {to_nick}, I can pull down my undies for you ^_^ ({total_count})",
            "follow me, {to_nick}, I have something I want to show you ^_^ ({total_count})",
            "wait here, {to_nick}, I'll be back with some supreme pussy for you ({total_count})",
            "for that amount of money, {to_nick}, I can only show you my tits ({total_count})",
            "ohh big spender, {to_nick}, here you have me fully undressed ({total_count})",
            "play nice, {to_nick}, and maybe I'll go down on my knees for you ({total_count})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            f"free pussy to all. When you find the key to my chastity belt. ^_^ ({total_count})",
            f"not enough money to supply you as well... ({total_count})",
            f"Nice try, but I don't think your IQ can handle that kind of complexity ({total_count})",
            f"Did your brain take a vacation when you typed that? ({total_count})",
            f"Did someone say 'purr-fect'? Because I'm all ears! ({total_count})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response) 
        
    @wrap([optional('something'), optional('channel')])
    def surprise(self, irc, msg, args, nickname, channel):
        """+surprise [nickname] - Serve a nice surprise. Optionally to a specific nickname."""

        # If a nickname is provided, use it; otherwise, use the caller's nickname (msg.nick)
        nick = nickname or msg.nick  
        address = msg.prefix
        network = irc.network

        # Check if the user is spamming commands
        if self._is_spamming(nick):
            last_served_time = self.last_command_time[nick]
            response = self._select_spam_reply(last_served_time)
            irc.reply(response)
            return        

        # Update stats
        today_count, total_count, sumtotal = self._update_stats(nick, address, "surprise", channel, network)

        # Responses for when a nickname is provided
        responses_with_nick = [
            "makes a sandwich for {to_nick}. enjoy this supreme dish ({today_count}/{total_count}/{sumtotal})",
            "pours a cup of fresh cow milk to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "goes out back and chopping the head off a chicken, so a great grilled chicken can be made for {to_nick}({today_count}/{total_count}/{sumtotal})",
        ]

        # Responses for when no nickname is provided
        responses_without_nick = [
            "GET OUT OF MY BAR! ({today_count}/{total_count}/{sumtotal})",
            "I only serve to people over 18+ ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if nickname:
            to_nick = f"{nick}"  # If nickname is provided, use the nickname
            # Select a random response from the list that includes {to_nick}
            response = random.choice(responses_with_nick).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        else:
            # Select a random response from the list that doesn't include {to_nick}
            response = random.choice(responses_without_nick).format(nick=nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)

        # Reply with the final formatted response
        irc.reply(response)        

Class = Serve