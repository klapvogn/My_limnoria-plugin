import paramiko
import supybot.utils as utils
import supybot.plugins as plugins
import supybot.callbacks as callbacks
import supybot.commands as commands

class Bandwidth(callbacks.Plugin):
    threaded = True

    def __init__(self, irc):
        super().__init__(irc)
        
        # SSH credentials for the remote machine
        self.ssh_host = '157.90.132.235'
        self.ssh_user = 'klapvogn'
        self.ssh_key = '/home/klapvogn/.ssh/id_ed25519.pub'  # Use key or password
        self.ssh_port = 50512  # Custom SSH port
        # self.ssh_password = 'yourpassword'  # Uncomment if using password

    def _get_bandwidth(self):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect via SSH
            client.connect(self.ssh_host, port=self.ssh_port, username=self.ssh_user, key_filename=self.ssh_key, timeout=5)
            
            # Run ifstat to get real-time bandwidth (1-second sample)
            stdin, stdout, stderr = client.exec_command("ifstat 1 1 | tail -n 1")
            output = stdout.read().decode().strip()
            client.close()
            
            if output:
                data = output.split()
                download_speed = float(data[0])  # First column is RX (kB/s)
                upload_speed = float(data[1])    # Second column is TX (kB/s)
                
                # Convert to kB/s and MB/s formatting
                down_str = f"{download_speed:.1f} kB/s" if download_speed < 1024 else f"{download_speed / 1024:.1f} MB/s"
                up_str = f"{upload_speed:.1f} kB/s" if upload_speed < 1024 else f"{upload_speed / 1024:.1f} MB/s"
                
                return f"Remote: Down: {down_str} UP: {up_str}"
            else:
                return "Error retrieving bandwidth data."
        except Exception as e:
            return f"Error: {str(e)}"

    def bw(self, irc, msg, args):
        """
        Fetches the current bandwidth usage from the remote server.
        """
        response = self._get_bandwidth()
        irc.reply(response)
    
    bw = commands.wrap(bw)  # Ensure the command is wrapped for Limnoria

Class = Bandwidth  # Ensure Limnoria recognizes the plugin