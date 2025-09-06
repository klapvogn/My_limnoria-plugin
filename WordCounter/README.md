## WordCounter

I made this plugin as a fun thing. All it does it's counts lines and words in channels that is defined in the script

# Settings

What channel(s) it should be used on:

`+config supybot.plugins.WordCounter.Channels #CHAN1 #CHAN2`

Exclude usernames, like bots

`+config supybot.plugins.WordCounter.Nicks nick1 nick2`


# Commands

`+stats - Shows the stats in the channel you are in`

`+stats 2025-07 (uses current channel)`

`+stats #CHAN - Shows the stats in the channel you are in`

`+stats #CHAN 2025-07`

# Output

```
Results for #CHAN - [YYY-MMM]
------------------------------------------------
Pos  | Username        |      Lines |      Words
------------------------------------------------
1    | Username1       |          4 |          8
2    | Username2       |          1 |          3
3    | Username3       |          1 |          1
4    | Username4       |          1 |          1
5    | Username5       |          1 |          1
------------------------------------------------
```