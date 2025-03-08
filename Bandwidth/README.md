## Bandwidth

I made this plugin as a fun thing. So when you would like to see what your bandwidth is on your remote computer, you can use +Bandwidth


## What you need

1: `pip install paramiko`

2: Then edit these lines:
```
        # SSH credentials for the remote machine
        self.ssh_host = 'REMOTE IP'
        self.ssh_user = 'REMOTE USERNAME'
        self.ssh_key = '/path/to/.ssh/publickey.pub'  # Use key or password
        self.ssh_port = REMOTE PORT  # Custom SSH port
        # self.ssh_password = 'yourpassword'  # Uncomment if using password
```

3: Then you should be able to use : `+bw`
