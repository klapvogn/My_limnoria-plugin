from supybot import callbacks, ircutils, ircmsgs, ircdb
from supybot.commands import wrap
import time
import collections

class Activity(callbacks.Plugin):
    """Plugin to show activity in the channel with a graph-like display."""
    
    def __init__(self, irc):
        self.__parent = super(Activity, self)
        self.__parent.__init__(irc)
        # Data structure to hold message counts per hour and per day (day of year)
        self.activity_data = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(int)))

    def doPrivmsg(self, irc, msg):
        """Hook into message events to record activity."""
        channel = msg.args[0]
        if ircutils.isChannel(channel):
            current_time = time.localtime()  # Use server's local time
            hour = current_time.tm_hour  # Get current local hour
            day = current_time.tm_yday  # Get the current day of the year
            self.activity_data[channel][day][hour] += 1  # Increment message count for this hour

    def _get_avg_activity(self, channel):
        """Calculate the average activity per hour over the last 28 days."""
        hour_sums = [0] * 24  # Array to store sums for each hour
        days_counted = 0

        # Get current day of the year
        current_day = time.localtime().tm_yday

        # Loop through the last 28 days
        for i in range(28):  # Consider last 28 days
            day = (current_day - i) % 365  # Handle wrap-around for day of year
            if day in self.activity_data[channel]:
                days_counted += 1
                for hour in range(24):
                    hour_sums[hour] += self.activity_data[channel][day].get(hour, 0)  # Sum the messages per hour

        # Return zero activity for all hours if no days were counted
        if days_counted == 0:
            return [0] * 24

        return [hour_sum // days_counted for hour_sum in hour_sums]  # Average per hour

    def _generate_graph(self, averages):
        """Generate a graph-like representation using characters."""
        symbols = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']  # From low to high activity
        max_avg = max(averages) or 1  # Avoid division by zero

        # Scale each average to one of the symbols
        graph = ''.join(symbols[min(int((avg / max_avg) * (len(symbols) - 1)), len(symbols) - 1)] for avg in averages)
        return graph

    @wrap(['channel'])
    def activity(self, irc, msg, args, channel):
        """Command to show the activity graph for the channel."""
        if channel not in self.activity_data:
            irc.reply("No activity data available for this channel.")
            return

        averages = self._get_avg_activity(channel)  # Get average activity data
        graph = self._generate_graph(averages)  # Generate the graph
        
        # Get the current time
        current_time = time.localtime()
        local_hour = current_time.tm_hour
        local_minute = current_time.tm_min

        # Format the time strings
        local_time_str = f"{local_hour:02d}:{local_minute:02d} local time"
        
        # Send the response to the IRC channel
        irc.reply(f'{graph} [avg msgs by hour (0-23) for {channel} (last 28 days) - currently, {local_time_str}]')

Class = Activity
