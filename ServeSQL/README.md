## Serve

What is Serve?

I made a bartender plugin that serves and a funny thing on iRC

It stores all the data in a sqlcipher database, where all the data is encrypted
and it resets all the 'today' stats at midnight (00:00) can be altered 

It has the following commands:

`+bar`

`Available on the menu: +anal, +lsd, +cola, +sprite, +fanta, +pepsi, +dew, +beer, +coffee, +redbull, +tea, +cap, +whiskey, +wine, +ice, +gumbo, +ganja, +mix, +head, +pipe, +coke, +pussy, +surprise, +tequila`


## Database
```
CREATE TABLE IF NOT EXISTS servestats (
  `id` int NOT NULL,
  `nick` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `address` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `last` double DEFAULT NULL,
  `today` int DEFAULT '0',
  `total` int DEFAULT '0',
  `channel` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `network` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;              
```
