import time
import psutil
import supybot
import supybot.utils as utils
import supybot.plugins as plugins
import supybot.callbacks as callbacks
import supybot.commands as commands

try:
    from supybot.i18n import PluginInternationalization
    _ = PluginInternationalization('Bandwidth')
except:
    # Placeholder for when translation isn't available
    def _(x): return x

class Bandwidth(callbacks.Plugin):
    """A plugin that monitors bandwidth usage."""
    threaded = True

    def __init__(self, irc):
        self.__parent = super(Bandwidth, self)
        self.__parent.__init__(irc)
        self.last_net_io = None
        self.last_time = None
        self.initialized = False

    def _get_bandwidth(self, interface=None):
        """Get current bandwidth usage in kB/s."""
        try:
            current_net_io = psutil.net_io_counters(pernic=True)
            current_time = time.time()
            
            if not self.initialized:
                self.last_net_io = current_net_io
                self.last_time = current_time
                self.initialized = True
                return None, None
            
            time_elapsed = current_time - self.last_time
            if time_elapsed <= 0:
                return None, None

            if interface:
                if interface not in current_net_io:
                    return None, None
                
                current = current_net_io[interface]
                last = self.last_net_io.get(interface)
                if last is None:
                    return None, None
                
                down = (current.bytes_recv - last.bytes_recv) / time_elapsed / 1024
                up = (current.bytes_sent - last.bytes_sent) / time_elapsed / 1024
            else:
                current_total = psutil.net_io_counters(pernic=False)
                last_total = self._sum_net_io(self.last_net_io)
                
                down = (current_total.bytes_recv - last_total.bytes_recv) / time_elapsed / 1024
                up = (current_total.bytes_sent - last_total.bytes_sent) / time_elapsed / 1024
            
            self.last_net_io = current_net_io
            self.last_time = current_time
            
            return down, up
        except Exception as e:
            self.log.error('Bandwidth error: %s', e)
            return None, None

    def _sum_net_io(self, net_io):
        """Sum network IO across all interfaces."""
        sent = sum(iface.bytes_sent for iface in net_io.values())
        recv = sum(iface.bytes_recv for iface in net_io.values())
        return type(next(iter(net_io.values())))(sent, recv, 0, 0, 0, 0, 0, 0)

    def bw(self, irc, msg, args, interface=None):
        """[<interface>]
        
        Shows current bandwidth usage. If interface is specified,
        shows usage for that interface only.
        """
        time.sleep(1)  # Wait for delta measurement
        down, up = self._get_bandwidth(interface)
        
        if down is None or up is None:
            irc.reply('Gathering initial data, please try again in a second.')
            return
        
        def format_speed(speed):
            if speed < 1024:
                return '%.1f kB/s' % speed
            return '%.1f MB/s' % (speed / 1024)
        
        if interface:
            irc.reply('Interface %s: Down: %s Up: %s' % (
                interface, format_speed(down), format_speed(up)))
        else:
            irc.reply('Total bandwidth: Down: %s Up: %s' % (
                format_speed(down), format_speed(up)))

    bw = commands.wrap(bw, [commands.optional('something')])

Class = Bandwidth