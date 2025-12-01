--
-- Table structure for table `releases`
--

CREATE TABLE `releases` (
  `id` int NOT NULL,
  `releasename` varchar(255) NOT NULL,
  `section` varchar(64) DEFAULT NULL,
  `unixtime` int DEFAULT (unix_timestamp()),
  `files` int DEFAULT '0',
  `size` decimal(10,2) DEFAULT '0.00',
  `grp` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `genre` varchar(100) DEFAULT NULL,
  `nuked` enum('1','2','3','4','5') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `reason` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `nukenet` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Indexes for table `releases`
--

ALTER TABLE `releases`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `releasename` (`releasename`),
  ADD KEY `unixtime_nuked` (`unixtime`,`nuked`),
  ADD KEY `stats_cover` (`nuked`,`unixtime`),
  ADD KEY `latest` (`unixtime` DESC);

--
-- AUTO_INCREMENT for table `releases`
--
ALTER TABLE `releases`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
