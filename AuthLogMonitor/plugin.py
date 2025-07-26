from supybot import utils, plugins, ircutils, callbacks
from supybot.commands import *
import os
import time
import threading
import supybot.ircmsgs as ircmsgs
import traceback
from chardet import detect

class AuthLogMonitor(callbacks.Plugin):
    """Monitors /var/log/auth.log and announces specific events to a channel"""
    threaded = True
    
    def __init__(self, irc):
        super().__init__(irc)
        self.filename = '/var/log/auth.log'
        self.file_position = 0
        self.announce_channel = '#authlogger'
        self.target_network = 'omg'
        self.running = True
        self.last_error_time = 0
        self.error_cooldown = 30
        self.debug = False  # Disable debug output in production
        self.last_activity_time = time.time()
        self.last_lines = set()  # Track recently seen lines to prevent duplicates     

        # Initialize to end of file
        if os.path.exists(self.filename):
            self.file_position = os.path.getsize(self.filename)
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self.monitor_loop, args=(irc,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def log_error(self, message, irc):
        """Log errors with cooldown to prevent spamming"""
        current_time = time.time()
        if current_time - self.last_error_time > self.error_cooldown:
            try:
                error_msg = f"[AuthLog Error] {message}"
                if self.debug:
                    print(error_msg)
                irc.queueMsg(ircmsgs.privmsg(self.announce_channel, error_msg))
                self.last_error_time = current_time
            except Exception as e:
                print(f"Failed to log error: {str(e)}")        

    def die(self):
        self.running = False
        if hasattr(self, 'monitor_thread') and self.monitor_thread.is_alive():
            self.monitor_thread.join(2)
        super().die()

    def monitor_loop(self, irc):
        while self.running:
            try:
                self.check_log_file(irc)
                time.sleep(1)
            except Exception as e:
                self.log_error(f"Monitor loop error: {str(e)}", irc)
                time.sleep(5)

    def send_message(self, irc, original_line):
        try:
            if not hasattr(irc, 'network') or self.target_network not in irc.network.lower():
                return

            prefix = "[Auth Log] "
            
            # 1. Handle authentication failure messages
            if 'pam_unix(' in original_line and 'authentication failure' in original_line:
                try:
                    # Extract service (su-l, sshd, etc)
                    service_start = original_line.index('pam_unix(') + 9
                    service_end = original_line.index(':', service_start)
                    service = original_line[service_start:service_end]

                    # Extract attempting user (user=root)
                    attempting_user = "unknown"
                    if 'user=' in original_line:
                        user_start = original_line.index('user=') + 5
                        user_end = original_line.find(' ', user_start)
                        attempting_user = original_line[user_start:user_end]

                    # Extract initiating user (ruser=klapvogn)
                    initiating_user = "unknown"
                    if 'ruser=' in original_line:
                        ruser_start = original_line.index('ruser=') + 6
                        ruser_end = original_line.find(' ', ruser_start)
                        initiating_user = original_line[ruser_start:ruser_end]

                    # Format message
                    msg = f"{prefix}Failed {service} auth: {initiating_user} tried to become {attempting_user}"
                    
                    irc.queueMsg(ircmsgs.privmsg(self.announce_channel, msg))
                    self.last_activity_time = time.time()
                    return
                except (ValueError, IndexError):
                    pass  # Fall through to default formatting

            # 2. Handle ALL session closed messages (simplified format)
            elif 'session closed for user' in original_line:
                try:
                    # More robust username extraction
                    session_part = original_line.split('session closed for user ')[1]
                    username = session_part.split('(')[0].strip()
                    
                    clean_msg = f"{prefix}session closed for user {username}"
                    irc.queueMsg(ircmsgs.privmsg(self.announce_channel, clean_msg))
                    self.last_activity_time = time.time()
                    return
                except (ValueError, IndexError, AttributeError) as e:
                    print(f"DEBUG: Failed to parse session closed message: {str(e)}")
                    print(f"DEBUG: Original line: {original_line}")
                    pass  # Fall through to other formats

            # 3. Handle session opened messages (detailed format)
            elif 'session opened for user' in original_line:
                try:
                    # Extract service (su-l, sshd, etc) if available
                    service = "unknown"
                    if 'pam_unix(' in original_line:
                        service_start = original_line.index('pam_unix(') + 9
                        service_end = original_line.index(':', service_start)
                        service = original_line[service_start:service_end]

                    # Extract username
                    user_start = original_line.index('session opened for user ') + 24
                    user_end = original_line.index('(', user_start)
                    username = original_line[user_start:user_end]

                    # Extract initiator if available
                    initiator = None
                    if ' by ' in original_line:
                        by_start = original_line.index(' by ') + 4
                        by_end = original_line.index('(', by_start)
                        initiator = original_line[by_start:by_end]

                    # Format message
                    if username.lower() == 'root':
                        msg = f"{prefix}ROOT session via {service} by {initiator}"
                    else:
                        msg = f"{prefix}Session for {username} via {service}"
                        if initiator:
                            msg += f" (by {initiator})"

                    irc.queueMsg(ircmsgs.privmsg(self.announce_channel, msg))
                    self.last_activity_time = time.time()
                    return
                except (ValueError, IndexError):
                    pass  # Fall through to default formatting

            # 4. Handle SSH logins (both publickey and password)
            elif 'Accepted publickey for' in original_line or 'Accepted password for' in original_line:
                parts = original_line.split()
                try:
                    username = parts[parts.index('for') + 1]
                    from_index = parts.index('from')
                    ip = parts[from_index + 1]
                    
                    # Obscure IP (show first two octets only)
                    ip_parts = ip.split('.')
                    obscured_ip = f"{ip_parts[0]}.{ip_parts[1]}.xx.xx" if len(ip_parts) >= 2 else ip
                    
                    # Get port if available
                    port = parts[from_index + 3] if parts[from_index + 2] == 'port' else ''
                    
                    clean_msg = f"{prefix}SSH login: {username} from {obscured_ip}"
                    if port:
                        clean_msg += f" port {port}"
                    
                    irc.queueMsg(ircmsgs.privmsg(self.announce_channel, clean_msg))
                    self.last_activity_time = time.time()
                    return
                except (ValueError, IndexError):
                    pass  # Fall through to default formatting

            # 5. Handle failed logins
            elif 'Failed password for' in original_line:
                parts = original_line.split()
                try:
                    username = parts[parts.index('for') + 1]
                    from_index = parts.index('from')
                    ip = parts[from_index + 1]
                    
                    # Obscure IP
                    ip_parts = ip.split('.')
                    obscured_ip = f"{ip_parts[0]}.{ip_parts[1]}.xx.xx" if len(ip_parts) >= 2 else ip
                    
                    clean_msg = f"{prefix}Failed login: {username} from {obscured_ip}"
                    irc.queueMsg(ircmsgs.privmsg(self.announce_channel, clean_msg))
                    self.last_activity_time = time.time()
                    return
                except (ValueError, IndexError):
                    pass  # Fall through to default formatting

            # 6. Handle disconnect messages
            elif 'Received disconnect from' in original_line:
                try:
                    parts = original_line.split()
                    from_index = parts.index('from')
                    ip = parts[from_index + 1]
                    port_index = parts.index('port')
                    port = parts[port_index + 1].rstrip(':')
                    reason = ' '.join(parts[port_index + 2:]).lstrip(':')
                    
                    # Obscure IP
                    ip_parts = ip.split('.')
                    obscured_ip = f"{ip_parts[0]}.{ip_parts[1]}.xx.xx" if len(ip_parts) >= 2 else ip
                    
                    clean_msg = f"{prefix}Disconnect from {obscured_ip} port {port}: {reason}"
                    irc.queueMsg(ircmsgs.privmsg(self.announce_channel, clean_msg))
                    self.last_activity_time = time.time()
                    return
                except (ValueError, IndexError):
                    pass  # Fall through to default formatting                

            # Default formatting for all other messages
            irc.queueMsg(ircmsgs.privmsg(self.announce_channel,
                    f"{prefix}{original_line[:300]}"))
                
        except Exception as e:
            self.log_error(f"Failed to send message: {str(e)}", irc)

    def check_log_file(self, irc):
        try:
            if not os.path.exists(self.filename):
                raise FileNotFoundError(f"File {self.filename} not found")
            
            if not os.access(self.filename, os.R_OK):
                raise PermissionError(f"No read permissions for {self.filename}")

            file_size = os.path.getsize(self.filename)
            
            # Always read from current position to end of file
            if file_size > self.file_position:
                with open(self.filename, 'rb') as f:
                    f.seek(self.file_position)
                    new_data = f.read(file_size - self.file_position)
                    
                    try:
                        encoding = detect(new_data)['encoding'] or 'utf-8'
                        lines = new_data.decode(encoding, errors='replace').splitlines()
                    except Exception as e:
                        raise Exception(f"Decoding failed: {str(e)}")

                    for line in lines:
                        line = line.strip()
                        if line and self.filter_auth_events(line):
                            # Skip if we've seen this line before
                            line_hash = hash(line)
                            if line_hash not in self.last_lines:
                                self.last_lines.add(line_hash)
                                self.send_message(irc, line)  # Pass the original line here
                    
                    # Keep only the most recent 1000 lines to prevent memory bloat
                    if len(self.last_lines) > 1000:
                        self.last_lines = set(list(self.last_lines)[-1000:])
                    
                    self.file_position = f.tell()
            elif file_size < self.file_position:
                # File was rotated, reset position
                self.file_position = 0
                self.last_lines = set()  # Clear seen lines cache
        
        except Exception as e:
            raise Exception(f"File check error: {str(e)}") from e

    def filter_auth_events(self, line):
        """More precise filtering to prevent spamming"""
        # Skip all cron-related root sessions and closures
        if ('session opened for user root' in line.lower() and ('via cron' in line.lower() or 'pam_unix(cron' in line.lower())) or \
        ('session closed for user root' in line.lower() and ('via cron' in line.lower() or 'pam_unix(cron' in line.lower())):
            return False
        
        interesting_events = [
            ('Accepted publickey for', True),
            ('Accepted password for', True),
            ('Failed password for', True),
            ('invalid user', True),
            ('Received disconnect from', True),
            ('Disconnected from user', True),

            ('session opened for user', True),  # More specific than just 'session opened'
            ('session closed for user', True),  # More specific than just 'session closed'
            ('sudo: ', False),  # Space after colon to avoid matching other words
            ('pam_unix(', True),  # Only match pam_unix function calls
        ]
        
        try:
            line_lower = line.lower()
            for pattern, active in interesting_events:
                if active and pattern.lower() in line_lower:
                    return True
            return False
        except Exception:
            return False
    
Class = AuthLogMonitor