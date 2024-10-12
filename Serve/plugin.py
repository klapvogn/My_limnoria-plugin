import sqlite3
import time
import random
from supybot.commands import *
import supybot.callbacks as callbacks

class Serve(callbacks.Plugin):
    def __init__(self, irc):
        self.__parent = super(Serve, self)
        self.__parent.__init__(irc)
        self.db = sqlite3.connect("/home/ubuntu/limnoria/plugins/Serve/servestats.db")
        self.init_db()

        # Settings for spam replies and date format
        self.settings = {
            "antispam": (1, 3),  # 1 trigger every 3 seconds
            "spamreplies": [
                "Hey hey there, don't you think it's going a bit too fast there? Only {since} sec, since your last ...",
                "I am busy ...",
                "Haven't you just had ?"
            ],
            "dateformat": "%H:%M:%S",  # Corresponds to 'h:i:s' in PHP
        }

        # Dictionary to track the last command time for each user
        self.last_command_time = {}

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

    def start_daily_clear(self):
        """Start a timer to clear daily stats at midnight."""
        now = time.time()
        midnight = time.mktime(time.strptime(time.strftime('%Y-%m-%d') + ' 00:00:00', '%Y-%m-%d %H:%M:%S')) + 86400
        delay = midnight - now
        if delay < 0:
            delay += 86400  # Schedule for tomorrow if midnight has passed today
        threading.Timer(delay, self.timer_serve).start()

    def timer_serve(self):
        """Clear daily stats and set the timer for the next day."""
        # Clear daily stats
        with self.db:
            self.db.execute("UPDATE servestats SET today = 0")
        self.ircClass.privMsg("#bot", "Cleared serve_today")

        # Start the timer for the next day
        self.start_daily_clear()            

    def _get_stats(self, nick, drink_type, channel, network):
        today = int(time.strftime('%Y%m%d'))
        cursor = self.db.cursor()

        cursor.execute('''SELECT today, SUM(total), COUNT(*)
                        FROM servestats
                        WHERE nick = ? AND type = ? AND channel = ? AND network = ?''',
                    (nick, drink_type, channel, network))

        result = cursor.fetchone()
        if result:
            today_count, total_count, sumtotal = result
            total_count = total_count or 0  # Ensure total_count is not None
        else:
            today_count, total_count, sumtotal = 0, 0, 0

        return today_count, total_count, sumtotal

    def _update_stats(self, nick, address, drink_type, channel, network):
        today = int(time.strftime('%Y%m%d'))
        cursor = self.db.cursor()

        today_count, total_count, sumtotal = self._get_stats(nick, drink_type, channel, network)

        if sumtotal == 0:
            # This means it's the first time this user has requested this drink
            today_count = 1  # Set today's count to 1
            total_count = 1  # Set total count to 1
            cursor.execute('''INSERT INTO servestats (nick, address, type, last, today, total, channel, network)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (nick, address, drink_type, time.time(), today_count, total_count, channel, network))
        else:
            if today_count == today:
                today_count += 1
            else:
                today_count = 1

            total_count += 1

            cursor.execute('''UPDATE servestats
                            SET last = ?, today = ?, total = ?
                            WHERE nick = ? AND type = ? AND channel = ? AND network = ?''',
                        (time.time(), today_count, total_count, nick, drink_type, channel, network))

        self.db.commit()
        return today_count, total_count, sumtotal + 1

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
# BAR

    @wrap([optional('channel')])
    def bar(self, irc, msg, args, channel):
        """+bar - Show all available drink commands."""
        # List of available drink commands
        available_drinks = [
            "+cola",     # Cola
            "+beer",     # Beer
            "+coffee",   # Coffee
            "+cap",      # Cappuccino
            "+whiskey",  # Whiskey
            "+wine",     # Wine
            "+ice",      # Ice cream            
            "+mix",      # Mix
            "+pipe",     # Pipe
            "+coke",     # Coke
            "+pussy",    # Pussy (from your existing code)
            "+surprise"  # Surprise
            # Add other drink commands here as needed
        ]

        # Format the response
        response = "Available drinks: " + ", ".join(available_drinks)
        
        # Send the response
        irc.reply(response)    

# DRINKS
    @wrap([optional('something'), optional('channel')])
    def cola(self, irc, msg, args, nickname, channel):
        """+cola [nickname] - Serve a cola. Optionally to a specific nickname."""
        
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "Serves icecold cola {to_nick}({today_count}/{total_count}/{sumtotal})",
            "Serves cola that has been laying in a pile of shit ~45c {to_nick}({today_count}/{total_count}/{sumtotal})",
            "Serves cola that's been standing close to a box of dry ice ~1.3c {to_nick}({today_count}/{total_count}/{sumtotal})",
            "Serves a warm flat cola that no one wants ~25c {to_nick}({today_count}/{total_count}/{sumtotal})",
            "Serves cola fresh from the fridge, chilled to perfection ~5c {to_nick}({today_count}/{total_count}/{sumtotal})",
            "Serves cola that tastes slightly metallic after standing in a can too long ~18c {to_nick}({today_count}/{total_count}/{sumtotal})"
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

    @wrap([optional('something'), optional('channel')])
    def beer(self, irc, msg, args, nickname, channel):
        """+beer [nickname] - Serve a cola. Optionally to a specific nickname."""
        
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "serves icecold beer {to_nick}({today_count}/{total_count}/{sumtotal})",
            "serves a beer that has been laying in a pile of shit ~45c {to_nick}({today_count}/{total_count}/{sumtotal})",
            "serves a warm beer {to_nick}({today_count}/{total_count}/{sumtotal})",
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
        today_count, total_count, sumtotal = self._update_stats(nick, address, "coffee", channel, network)

        # Add ordinal suffix to sumtotal
        ordinal_suffix = self.add_ordinal_number_suffix(sumtotal)

        responses = [
            f"Making a cup of coffee for {nick}, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made coffee."
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
        today_count, total_count, sumtotal = self._update_stats(nick, address, "cap", channel, network)

        # Add ordinal suffix to sumtotal
        ordinal_suffix = self.add_ordinal_number_suffix(sumtotal)

        responses = [
            f"Making a nice Cappuccino for {nick}, {today_count} made today out of {total_count} ordered, making it the {ordinal_suffix} time I made Cappuccino."
        ]

        # Select a random response
        response = random.choice(responses)
        irc.reply(response)        

    @wrap([optional('something'), optional('channel')])
    def whiskey(self, irc, msg, args, nickname, channel):
        """+whiskey [nickname] - Serve a cola. Optionally to a specific nickname."""
        
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "serves whiskey on the rocks {to_nick}({today_count}/{total_count}/{sumtotal})",
            "found some weird looking bottle in corner, might hit gold cheers {to_nick}({today_count}/{total_count}/{sumtotal})",
            "cola and bad whiskey for you {to_nick}({today_count}/{total_count}/{sumtotal})",
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

    @wrap([optional('something'), optional('channel')])
    def wine(self, irc, msg, args, nickname, channel):
        """+wine [nickname] - Serve a cola. Optionally to a specific nickname."""
        
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "pours up some fine stuff from the basement {to_nick}({today_count}/{total_count}/{sumtotal})",
            "here you are, found something out back {to_nick}({today_count}/{total_count}/{sumtotal})",
            "lucky you we just got one of this left enjoy {to_nick}({today_count}/{total_count}/{sumtotal})",
            "so you're hit hard, where you want it ?, don't cry {to_nick}({today_count}/{total_count}/{sumtotal})",
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "here {to_nick}... one ball for you only ({today_count}/{total_count}/{sumtotal})",
            "finds a biig icecream for {to_nick} eat and you get for free ($50 to use toilet) ({today_count}/{total_count}/{sumtotal})",
            "dusts off something that look like icecream from the corner of fridge, here you go {to_nick}({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if not nickname:
            to_nick = ""  # No nickname, just empty string
        else:
            to_nick = f"{nick} "  # If nickname is provided, add "to <nick>"

        # Select a random response and format it with stats and nickname info
        response = random.choice(responses).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        
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
            f"grinding up some weed for a mix ({total_count})",
            f"grabs some of the good stuff for a mix ({total_count})",
            f"sneaks into {random_name}'s stash and steals for a mix, here you go ({total_count})",
            f"goes strain hunting in India for some good shit for your mix ({total_count})",
            f"goes strain hunting in Morocco for some good shit for your mix ({total_count})",
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
            "goes strain hunting in morocco for some good shit for your pipe {to_nick}({today_count}/{total_count}/{sumtotal})",
            "saw some shit in corner, fills a pipe {to_nick}({today_count}/{total_count}/{sumtotal})",
            "skunky just arrieved peace all over {to_nick}({today_count}/{total_count}/{sumtotal})",
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
        """+coke - you need some pussy"""
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
# END

    @wrap([optional('something'), optional('channel')])
    def pussy(self, irc, msg, args, nickname, channel):
        """+pussy [nickname] - Serve a cola. Optionally to a specific nickname."""
        
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "slaps {to_nick}in face with a smelly pussy ({today_count}/{total_count}/{sumtotal})",
            "sends some pussy {to_nick}'s way.. ({today_count}/{total_count}/{sumtotal})",
            f"not enough money to suply you aswell ... ({today_count}/{total_count}/{sumtotal})",
            "if you have the cash {to_nick}I can pull down my undies for you ^_^ ({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if not nickname:
            to_nick = ""  # No nickname, just empty string
        else:
            to_nick = f"{nick} "  # If nickname is provided, add "to <nick>"

        # Select a random response and format it with stats and nickname info
        response = random.choice(responses).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        
        # Reply with the final formatted response
        irc.reply(response)
        
    @wrap([optional('something'), optional('channel')])
    def surprise(self, irc, msg, args, nickname, channel):
        """+surprise [nickname] - Serve a surprise. Optionally to a specific nickname."""
        
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

        # Define response templates with a placeholder for nickname (or lack thereof)
        responses = [
            "makes a sandwich for {to_nick} enjoy this suprime dish ({today_count}/{total_count}/{sumtotal})",
            "pours a cup of fresh cow milk to {to_nick} ({today_count}/{total_count}/{sumtotal})",
            "goes out back and chopping the head off a chicken, so a great grilled chicken can be made for {to_nick}({today_count}/{total_count}/{sumtotal})",
        ]

        # Handle the case of nickname or not, to adjust the {to_nick} part
        if not nickname:
            to_nick = ""  # No nickname, just empty string
        else:
            to_nick = f"{nick} "  # If nickname is provided, add "to <nick>"

        # Select a random response and format it with stats and nickname info
        response = random.choice(responses).format(to_nick=to_nick, today_count=today_count, total_count=total_count, sumtotal=sumtotal)
        
        # Reply with the final formatted response
        irc.reply(response)         

Class = Serve
