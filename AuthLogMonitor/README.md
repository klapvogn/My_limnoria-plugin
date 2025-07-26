## AuthLogMonitor

I made thing plugin, so I could see what happens in the `/var/log/auth.log` that is importens for some


## What you need

`pip install chardet` 

Read more about here: 
[chardet](https://pypi.org/project/chardet/)

The plugin autoload on restart of the bot

# Outputs

When you login to your server (ssh)
by default the code is made so it : Obscure IP (show first two octets only)

`[Auth Log] SSH login: user from xx.xx.xx.xx port 52446`

`Session for user via sshd (by user)`

`session closed for user user`

# Disconncted

`Disconnect from xx.xx.xx.xx port 16531:11: disconnected by user`

