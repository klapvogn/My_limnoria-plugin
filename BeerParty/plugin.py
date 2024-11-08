import pytz
from datetime import datetime, time as dt_time, timedelta
import threading
import random
from supybot import callbacks, ircmsgs, schedule

class BeerParty(callbacks.Plugin):
    def __init__(self, irc):
        super().__init__(irc)
        self.excluded_nicks = ["chat"]
        self.beer_party_sent_today = False  # Flag to track if the beer party message has been sent today
        self.schedule_daily_6pm_reset(irc)  # Schedule the first beer party when the plugin loads

        # Schedule to reset the daily flag at midnight
        self.schedule_daily_reset()

    def schedule_daily_reset(self):
        local_tz = pytz.timezone('Europe/Copenhagen')
        now = datetime.now(local_tz)
        
        # Calculate time until midnight
        midnight = local_tz.localize(datetime.combine(now.date() + timedelta(days=1), dt_time(0, 0)))
        seconds_until_midnight = (midnight - now).total_seconds()

        # Schedule the reset of the daily flag at midnight
        self.reset_timer = threading.Timer(seconds_until_midnight, self.reset_daily_flag)
        self.reset_timer.start()

    def reset_daily_flag(self):
        # Reset the flag at the start of each new day
        self.beer_party_sent_today = False
        self.schedule_daily_reset()  # Reschedule the daily reset for the next day

    def schedule_daily_6pm_reset(self, irc):
        try:
            # Set timezone to Copenhagen
            local_tz = pytz.timezone('Europe/Copenhagen')
            now = datetime.now(local_tz)

            # Target time is 18:00 in the local timezone
            scheduled_time = local_tz.localize(datetime.combine(now.date(), dt_time(18, 0)))

            # If it's already past 18:00, schedule for the next day
            if now >= scheduled_time:
                scheduled_time += timedelta(days=1)

            # Calculate the seconds until the next scheduled time
            seconds_until_scheduled_time = (scheduled_time - now).total_seconds()
            
            # Log current time and next scheduled beer party time
            self.log.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}, Next beer party at: {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                          f"Seconds until next event: {seconds_until_scheduled_time:.2f}")

            # Schedule the beer party at 18:00 with threading.Timer
            self.timer = threading.Timer(seconds_until_scheduled_time, lambda: self.beer_party(irc))
            self.timer.start()
        except Exception as e:
            self.log.error(f"Error occurred while scheduling daily beer party: {str(e)}")

    def beer_party(self, irc):
        if self.beer_party_sent_today:
            self.log.info("Beer party message has already been sent today. Skipping...")
            return

        current_time = datetime.now()
        self.log.info(f"Beer party triggered at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Specify the target channel
        target_channel = '#omgwtfnzbs.chat'

        # Check if the bot is in the target channel
        if target_channel not in irc.state.channels:
            self.log.error(f"Channel {target_channel} not found in IRC state. Attempting to rejoin.")
            irc.sendMsg(ircmsgs.join(target_channel))  # Try to join the channel if not present
            return  # Skip this attempt and retry on the next scheduled event

        # Proceed with selecting a payer and sending the message if channel check passes
        users = list(irc.state.channels[target_channel].users)
        eligible_users = [user for user in users if user not in self.excluded_nicks]

        if eligible_users:
            payer = random.choice(eligible_users)
            irc.queueMsg(ircmsgs.privmsg(target_channel, f"Free beer for everyone 🍺🍻! {payer} is paying!"))
        else:
            irc.queueMsg(ircmsgs.privmsg(target_channel, "No one is here to pay for the beer!"))

        self.beer_party_sent_today = True
        self.log.info("Beer party message sent successfully. Flag set to True.")
        self.schedule_daily_6pm_reset(irc)


Class = BeerParty
