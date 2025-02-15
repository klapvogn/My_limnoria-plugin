## Serve

What is Serve?

I made a bartender plugin that serves and a funny thing on iRC

It stores all the data in a sqlite3 database
and it resets all the 'today' stats at midnight (00:00) can be altered 

It has the following commands:

`+bar`

`Available on the menu: +anal, +lsd, +cola, +sprite, +fanta, +pepsi, +dew, +beer, +coffee, +redbull, +tea, +cap, +whiskey, +wine, +ice, +gumbo, +ganja, +mix, +head, +pipe, +coke, +pussy, +surprise, +tequila`


## Database
CREATE TABLE IF NOT EXISTS servestats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nick TEXT NOT NULL,
                address TEXT NOT NULL,
                type TEXT NOT NULL,
                last REAL NOT NULL,
                today INTEGER NOT NULL,
                total INTEGER NOT NULL,
                channel TEXT NOT NULL,
                network TEXT NOT NULL
);                
