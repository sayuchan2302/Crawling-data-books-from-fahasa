-- MySQL dump 10.13  Distrib 8.0.44, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: fahasa_datamart
-- ------------------------------------------------------
-- Server version	9.5.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
SET @@SESSION.SQL_LOG_BIN= 0;

--
-- GTID state at the beginning of the backup 
--

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'a78e9c2b-c485-11f0-9e54-00155d63397d:1-13138';

--
-- Table structure for table `mart_author_insights`
--

DROP TABLE IF EXISTS `mart_author_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_author_insights` (
  `author_id` int NOT NULL,
  `author_name` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total_books` int NOT NULL DEFAULT '0',
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `most_popular_book` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`author_id`),
  KEY `idx_author_name` (`author_name`),
  KEY `idx_avg_rating` (`avg_rating`),
  KEY `idx_total_revenue` (`total_revenue`),
  KEY `idx_last_updated` (`last_updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Author performance insights and metrics for BI reporting';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_category_performance`
--

DROP TABLE IF EXISTS `mart_category_performance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_category_performance` (
  `category_id` int NOT NULL,
  `category_path` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total_books` int NOT NULL DEFAULT '0',
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `avg_price` decimal(12,2) DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `market_share` decimal(5,2) DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`category_id`),
  KEY `idx_category_path` (`category_path`),
  KEY `idx_total_revenue` (`total_revenue`),
  KEY `idx_market_share` (`market_share`),
  KEY `idx_last_updated` (`last_updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Category performance metrics for business analysis';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_daily_sales`
--

DROP TABLE IF EXISTS `mart_daily_sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_daily_sales` (
  `id` int NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `product_id` int DEFAULT NULL,
  `product_name` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_path` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publisher_name` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `author_names` text COLLATE utf8mb4_unicode_ci,
  `price` decimal(12,2) DEFAULT NULL,
  `discount_price` decimal(12,2) DEFAULT NULL,
  `discount_percent` decimal(5,2) DEFAULT NULL,
  `rating` decimal(3,2) DEFAULT NULL,
  `rating_count` int DEFAULT NULL,
  `sold_today` int DEFAULT NULL,
  `sold_cumulative` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_date_product` (`date`,`product_id`),
  KEY `idx_date` (`date`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_category_path` (`category_path`),
  KEY `idx_publisher_name` (`publisher_name`),
  KEY `idx_price` (`price`),
  KEY `idx_rating` (`rating`),
  KEY `idx_sold_today` (`sold_today`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Daily sales tracking for operational reporting and trend analysis';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_product_flat`
--

DROP TABLE IF EXISTS `mart_product_flat`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_product_flat` (
  `product_id` int NOT NULL,
  `product_name` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL,
  `isbn` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_path` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `author_names` text COLLATE utf8mb4_unicode_ci,
  `publisher_name` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `page_count` int DEFAULT NULL,
  `weight` decimal(8,2) DEFAULT NULL,
  `dimensions` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `language` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `first_seen` date DEFAULT NULL,
  `last_seen` date DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `avg_price` decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (`product_id`),
  KEY `idx_product_name` (`product_name`(100)),
  KEY `idx_isbn` (`isbn`),
  KEY `idx_category_path` (`category_path`),
  KEY `idx_publisher_name` (`publisher_name`),
  KEY `idx_avg_rating` (`avg_rating`),
  KEY `idx_total_sold` (`total_sold`),
  KEY `idx_first_seen` (`first_seen`),
  KEY `idx_last_seen` (`last_seen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Flattened product view with all attributes for easy reporting and analysis';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_publisher_performance`
--

DROP TABLE IF EXISTS `mart_publisher_performance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_publisher_performance` (
  `publisher_id` int NOT NULL,
  `publisher_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total_books` int NOT NULL DEFAULT '0',
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `market_share` decimal(5,2) DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`publisher_id`),
  KEY `idx_publisher_name` (`publisher_name`),
  KEY `idx_total_revenue` (`total_revenue`),
  KEY `idx_market_share` (`market_share`),
  KEY `idx_avg_rating` (`avg_rating`),
  KEY `idx_last_updated` (`last_updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Publisher performance metrics for business intelligence';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `view_category_summary`
--

DROP TABLE IF EXISTS `view_category_summary`;
/*!50001 DROP VIEW IF EXISTS `view_category_summary`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `view_category_summary` AS SELECT 
 1 AS `category_path`,
 1 AS `total_books`,
 1 AS `avg_rating`,
 1 AS `avg_price`,
 1 AS `total_sold`,
 1 AS `total_revenue`,
 1 AS `market_share`,
 1 AS `market_position`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `view_daily_sales_trends`
--

DROP TABLE IF EXISTS `view_daily_sales_trends`;
/*!50001 DROP VIEW IF EXISTS `view_daily_sales_trends`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `view_daily_sales_trends` AS SELECT 
 1 AS `date`,
 1 AS `products_sold`,
 1 AS `total_books_sold`,
 1 AS `total_revenue`,
 1 AS `avg_rating`,
 1 AS `avg_price`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `view_product_dashboard`
--

DROP TABLE IF EXISTS `view_product_dashboard`;
/*!50001 DROP VIEW IF EXISTS `view_product_dashboard`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `view_product_dashboard` AS SELECT 
 1 AS `product_id`,
 1 AS `product_name`,
 1 AS `category_path`,
 1 AS `publisher_name`,
 1 AS `total_sold`,
 1 AS `avg_rating`,
 1 AS `avg_price`,
 1 AS `sales_category`,
 1 AS `rating_category`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `view_top_authors_revenue`
--

DROP TABLE IF EXISTS `view_top_authors_revenue`;
/*!50001 DROP VIEW IF EXISTS `view_top_authors_revenue`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `view_top_authors_revenue` AS SELECT 
 1 AS `author_id`,
 1 AS `author_name`,
 1 AS `total_books`,
 1 AS `avg_rating`,
 1 AS `total_sold`,
 1 AS `total_revenue`,
 1 AS `revenue_rank`*/;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `view_category_summary`
--

/*!50001 DROP VIEW IF EXISTS `view_category_summary`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `view_category_summary` AS select `mart_category_performance`.`category_path` AS `category_path`,`mart_category_performance`.`total_books` AS `total_books`,`mart_category_performance`.`avg_rating` AS `avg_rating`,`mart_category_performance`.`avg_price` AS `avg_price`,`mart_category_performance`.`total_sold` AS `total_sold`,`mart_category_performance`.`total_revenue` AS `total_revenue`,`mart_category_performance`.`market_share` AS `market_share`,(case when (`mart_category_performance`.`market_share` >= 10) then 'High' when (`mart_category_performance`.`market_share` >= 5) then 'Medium' else 'Low' end) AS `market_position` from `mart_category_performance` order by `mart_category_performance`.`total_revenue` desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `view_daily_sales_trends`
--

/*!50001 DROP VIEW IF EXISTS `view_daily_sales_trends`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `view_daily_sales_trends` AS select `mart_daily_sales`.`date` AS `date`,count(distinct `mart_daily_sales`.`product_id`) AS `products_sold`,sum(`mart_daily_sales`.`sold_today`) AS `total_books_sold`,sum((`mart_daily_sales`.`sold_today` * `mart_daily_sales`.`price`)) AS `total_revenue`,avg(`mart_daily_sales`.`rating`) AS `avg_rating`,avg(`mart_daily_sales`.`price`) AS `avg_price` from `mart_daily_sales` where (`mart_daily_sales`.`date` >= (curdate() - interval 30 day)) group by `mart_daily_sales`.`date` order by `mart_daily_sales`.`date` desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `view_product_dashboard`
--

/*!50001 DROP VIEW IF EXISTS `view_product_dashboard`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `view_product_dashboard` AS select `mart_product_flat`.`product_id` AS `product_id`,`mart_product_flat`.`product_name` AS `product_name`,`mart_product_flat`.`category_path` AS `category_path`,`mart_product_flat`.`publisher_name` AS `publisher_name`,`mart_product_flat`.`total_sold` AS `total_sold`,`mart_product_flat`.`avg_rating` AS `avg_rating`,`mart_product_flat`.`avg_price` AS `avg_price`,(case when (`mart_product_flat`.`total_sold` >= 1000) then 'Bestseller' when (`mart_product_flat`.`total_sold` >= 500) then 'Popular' when (`mart_product_flat`.`total_sold` >= 100) then 'Moderate' else 'Slow' end) AS `sales_category`,(case when (`mart_product_flat`.`avg_rating` >= 4.5) then 'Excellent' when (`mart_product_flat`.`avg_rating` >= 4.0) then 'Good' when (`mart_product_flat`.`avg_rating` >= 3.5) then 'Average' else 'Below Average' end) AS `rating_category` from `mart_product_flat` where (`mart_product_flat`.`total_sold` > 0) order by `mart_product_flat`.`total_sold` desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `view_top_authors_revenue`
--

/*!50001 DROP VIEW IF EXISTS `view_top_authors_revenue`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `view_top_authors_revenue` AS select `mart_author_insights`.`author_id` AS `author_id`,`mart_author_insights`.`author_name` AS `author_name`,`mart_author_insights`.`total_books` AS `total_books`,`mart_author_insights`.`avg_rating` AS `avg_rating`,`mart_author_insights`.`total_sold` AS `total_sold`,`mart_author_insights`.`total_revenue` AS `total_revenue`,rank() OVER (ORDER BY `mart_author_insights`.`total_revenue` desc )  AS `revenue_rank` from `mart_author_insights` where (`mart_author_insights`.`total_revenue` is not null) order by `mart_author_insights`.`total_revenue` desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-11-25 16:20:11
