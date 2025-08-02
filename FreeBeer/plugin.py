import pytz
from datetime import datetime, time as dt_time, timedelta
from threading import Timer
import random
from supybot import callbacks, ircmsgs

class BeerParty(callbacks.Plugin):
    def __init__(self, irc):
        super().__init__(irc)
        self.excluded_nicks = ["chat"]
        self.target_channel = '#omgwtfnzbs.chat'
        
        # Non-persistent flag (will reset on bot restart)
        self.beer_party_sent_today = False
        
        # Initialize scheduler
        self._init_scheduler(irc)

    def _init_scheduler(self, irc):
        """Initialize all scheduled events"""
        self._schedule_daily_reset()
        self._schedule_daily_beer_party(irc)

    def _schedule_daily_reset(self):
        """Schedule midnight reset of daily flag"""
        now = self._current_copenhagen_time()
        midnight = self._next_midnight(now)
        delay = (midnight - now).total_seconds()
        
        self.log.debug(f"Scheduling daily reset for {midnight} (in {delay} seconds)")
        
        # Cancel any existing timer
        if hasattr(self, 'reset_timer'):
            self.reset_timer.cancel()
            
        self.reset_timer = Timer(delay, self._reset_daily_flag)
        self.reset_timer.daemon = True
        self.reset_timer.start()

    def _schedule_daily_beer_party(self, irc):
        """Schedule the 6pm beer party"""
        now = self._current_copenhagen_time()
        beer_time = self._next_beer_time(now)
        delay = (beer_time - now).total_seconds()
        
        self.log.debug(f"Scheduling beer party for {beer_time} (in {delay} seconds)")
        
        # Cancel any existing timer
        if hasattr(self, 'beer_timer'):
            self.beer_timer.cancel()
            
        self.beer_timer = Timer(delay, self._trigger_beer_party, [irc])
        self.beer_timer.daemon = True
        self.beer_timer.start()

    def _current_copenhagen_time(self):
        """Get current time in Copenhagen timezone"""
        return datetime.now(pytz.timezone('Europe/Copenhagen'))

    def _next_midnight(self, now):
        """Calculate next midnight in Copenhagen time"""
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    def _next_beer_time(self, now):
        """Calculate next 6pm in Copenhagen time"""
        beer_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
        return beer_time if beer_time > now else beer_time + timedelta(days=1)

    def _reset_daily_flag(self):
        """Reset the daily flag at midnight"""
        self.beer_party_sent_today = False
        self.log.info("Daily beer party flag reset at midnight")
        self._schedule_daily_reset()  # Reschedule for next day

    def _trigger_beer_party(self, irc):
        """Handle the beer party event"""
        try:
            if self.beer_party_sent_today:
                self.log.debug("Beer party already sent today")
                return

            if not self._ensure_in_channel(irc):
                self.log.error("Failed to join channel, retrying in 1 hour")
                self._retry_later(irc)
                return

            self._send_beer_message(irc)
            self.beer_party_sent_today = True
            self.log.info("Beer party message sent successfully")
            
        except Exception as e:
            self.log.error(f"Error in beer party: {e}")
            self._retry_later(irc)
        finally:
            self._schedule_daily_beer_party(irc)

    def _ensure_in_channel(self, irc):
        """Ensure bot is in target channel"""
        if self.target_channel not in irc.state.channels:
            self.log.info(f"Joining channel {self.target_channel}")
            irc.queueMsg(ircmsgs.join(self.target_channel))
            return False  # Assume we need to wait for join to complete
        return True

    def _retry_later(self, irc, delay=3600):
        """Retry beer party after delay (default 1 hour)"""
        self.log.info(f"Scheduling retry in {delay} seconds")
        retry_timer = Timer(delay, self._trigger_beer_party, [irc])
        retry_timer.daemon = True
        retry_timer.start()

    def _send_beer_message(self, irc):
        """Send the actual beer message to channel"""
        users = list(irc.state.channels[self.target_channel].users)
        eligible_users = [user for user in users if user not in self.excluded_nicks]

        if eligible_users:
            payer = random.choice(eligible_users)
            msg = f"Free beer for everyone 🍺🍻! {payer} is paying!"
        else:
            msg = "No one is here to pay for the beer!"

        irc.queueMsg(ircmsgs.privmsg(self.target_channel, msg))

    def die(self):
        """Clean up when plugin is unloaded"""
        if hasattr(self, 'reset_timer'):
            self.reset_timer.cancel()
        if hasattr(self, 'beer_timer'):
            self.beer_timer.cancel()
        super().die()

Class = BeerParty