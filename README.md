# Here is how to install Limnoria

# Global installation (with root access)

If you do not have root access, skip this section.

If you are logged in as root, you can remove `sudo` from the install commands.

First, install Limnoria’s optional dependencies (you can skip this step, but some features won’t be available):

```
sudo python3 -m pip install -r https://raw.githubusercontent.com/progval/Limnoria/master/requirements.txt --upgrade
```

Then Limnoria itself:

```
sudo python3 -m pip install limnoria --upgrade
```

If you have an error saying `No module named pip`, install pip using your package manager (the package is usually named `python3-pip`).
If you have an error about `externally-managed-environment`, you need to setup a virtualenv first, then re-run the commands above:
```
python3 -m venv /opt/venvs/limnoria  # creates a virtualenv at the given path
. /opt/venvs/limnoria/bin/activate   # enables the virtualenv in the current shell
```

# Local installation (without root access)
If you have followed the previous section, skip this one.

First we install requirements (you can skip it, but some features won’t be available) and then Limnoria itself.:
```
python3 -m pip install -r https://raw.githubusercontent.com/progval/Limnoria/master/requirements.txt --upgrade
python3 -m pip install limnoria --upgrade
```
You might need to add $HOME/.local/bin to your PATH.:
```
echo 'PATH="$HOME/.local/bin:$PATH"' >> ~/.$(echo $SHELL|cut -d/ -f3)rc
source ~/.$(echo $SHELL|cut -d/ -f3)rc
```
If you have an error saying `No module named pip`, install pip using this guide:

https://pip.pypa.io/en/stable/installing/

Now to start the bot, run, still from within the ‘runbot’ directory:
```
supybot yourbotnick.conf
```
And watch the magic!


[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=QC2EH6ZRDL37L)

# Limnoria plugins I wrote or forked, and added more to it.

Limnoria (an IRC bot) plugins I wrote or forked. All working under Python 3. 

Requires [Limnoria](https://github.com/ProgVal/Limnoria), obviously. Additional requirements in requirements.txt files

Plugins assume Python 3.6+, though many may still work with older versions.


See README files in plugin directories for additional information and instructions.

[![License: WTFPL](https://img.shields.io/badge/license-WTFPL-brightgreen.svg)](http://www.wtfpl.net/about/) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
