## Kudos

What is Kudos?

Kudos is used to award positive and negative kudo points to users in a channel. e.g. bob++ will award a positive point to bob, and -- a negative point. More than one point may be
awarded by using, for example, bob+++ or bob+=2, however, this is limited by your current level of kudos (positive-negative kudos); 1 additional point for every 50 kudo points you
have (max 3). You can also rate

The script use sqlite3 database and are being made in your Kudos directory. 
You just need to edit

`self.conn = sqlite3.connect('/path/to/Kudos/kudos.db')  # Create or connect to a database file`

## Categories
```
'i': 'Informative'
'f': 'Funny'
'n': 'Nerd'
't': 'Troll'
'w': 'Wrong'
```

## Use it like this:

```
<nick>++i (The letter "i" can be substituted with "f,n,t,w")
<nick>: +4, -1 = 3. Informative (1)
```
## Stats
```
+scores
<nick>: 3 (Informative: 4 - Funny: 0 - Nerd: 0 - Troll: 0) / (Wrong: -0 - Troll: -1) (positive: 4, negative: 1)
```
```
+score <nick>
<nick> has 4 positive and 1 negative kudos. (Net score: 3)
```
