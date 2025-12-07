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
CREATE TABLE `servestats` (
  `id` int NOT NULL AUTO_INCREMENT,
  `nick` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `address` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `last` double DEFAULT NULL,
  `today` int DEFAULT '0',
  `total` int DEFAULT '0',
  `channel` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `network` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Stores service statistics tracking';
--
-- Indexes for table `servestats`
--
ALTER TABLE `servestats`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `unique_serve` (`nick`,`type`,`channel`,`network`);


/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;         
```

