import pytz
from datetime import datetime, time as dt_time, timedelta
import threading
import random
from supybot import callbacks, ircmsgs, schedule

class BeerParty(callbacks.Plugin):
    def __init__(self, irc):
        super().__init__(irc)
        self.excluded_nicks = ["chat"]
        self.schedule_daily_6pm_reset(irc)

    def schedule_daily_6pm_reset(self, irc):
        try:
            local_tz = pytz.timezone('Europe/Copenhagen')  # Set your local timezone
            now = datetime.now(local_tz)
            scheduled_time = local_tz.localize(datetime.combine(now.date(), dt_time(18, 00)))  # 18:00 every day

            # Schedule for the next day at 18:00 if it's already past today's 18:00
            if now >= scheduled_time:
                scheduled_time += timedelta(days=1)

            seconds_until_scheduled_time = (scheduled_time - now).total_seconds()
            self.log.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}, Next beer party at: {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}, "
                          f"Seconds until next event: {seconds_until_scheduled_time:.2f}")

            # Schedule the beer party task with irc parameter
            self.timer = threading.Timer(seconds_until_scheduled_time, lambda: self.beer_party(irc))
            self.timer.start()
        except Exception as e:
            self.log.error(f"Error occurred while scheduling daily beer party: {str(e)}")

    def beer_party(self, irc):
        current_time = datetime.now()
        self.log.info(f"Beer party triggered at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Specify the channel to send the message to
        target_channel = '#omgwtfnzbs.chat'
        
        # Get eligible users
        users = list(irc.state.channels[target_channel].users)
        eligible_users = [user for user in users if user not in self.excluded_nicks]

        # Send message in the specified channel
        if eligible_users:
            payer = random.choice(eligible_users)
            irc.queueMsg(ircmsgs.privmsg(target_channel, f"Free beer for everyone 🍺🍻! {payer} is paying!"))
        else:
            irc.queueMsg(ircmsgs.privmsg(target_channel, "No one is here to pay for the beer!"))

        # Reschedule for the next day at 18:00
        self.schedule_daily_6pm_reset(irc)

Class = BeerParty
