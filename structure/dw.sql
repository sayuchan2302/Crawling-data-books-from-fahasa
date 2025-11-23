-- MySQL dump 10.13  Distrib 8.0.44, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: fahasa_dw
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

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'a78e9c2b-c485-11f0-9e54-00155d63397d:1-11659';

--
-- Table structure for table `author_dim`
--

DROP TABLE IF EXISTS `author_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `author_dim` (
  `author_id` int NOT NULL AUTO_INCREMENT,
  `author_name` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`author_id`),
  UNIQUE KEY `uq_author_name` (`author_name`),
  KEY `idx_author_name` (`author_name`)
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `author_performance_aggregate`
--

DROP TABLE IF EXISTS `author_performance_aggregate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `author_performance_aggregate` (
  `author_id` int NOT NULL,
  `total_books` int DEFAULT '0',
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `total_sold` int DEFAULT '0',
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`author_id`),
  KEY `idx_total_books` (`total_books`),
  KEY `idx_avg_rating` (`avg_rating`),
  KEY `idx_total_revenue` (`total_revenue`),
  CONSTRAINT `author_performance_aggregate_ibfk_1` FOREIGN KEY (`author_id`) REFERENCES `author_dim` (`author_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `category_dim`
--

DROP TABLE IF EXISTS `category_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `category_dim` (
  `category_id` int NOT NULL AUTO_INCREMENT,
  `level_1` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `level_2` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `level_3` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_path` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`category_id`),
  KEY `idx_level_1` (`level_1`),
  KEY `idx_category_path` (`category_path`(100))
) ENGINE=InnoDB AUTO_INCREMENT=151 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `category_sales_aggregate`
--

DROP TABLE IF EXISTS `category_sales_aggregate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `category_sales_aggregate` (
  `category_id` int NOT NULL,
  `total_books` int DEFAULT '0',
  `avg_price` decimal(12,2) DEFAULT NULL,
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `total_sold` int DEFAULT '0',
  `market_share` decimal(5,2) DEFAULT NULL,
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`category_id`),
  KEY `idx_total_books` (`total_books`),
  KEY `idx_market_share` (`market_share`),
  CONSTRAINT `category_sales_aggregate_ibfk_1` FOREIGN KEY (`category_id`) REFERENCES `category_dim` (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `date_dim`
--

DROP TABLE IF EXISTS `date_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `date_dim` (
  `date_sk` int NOT NULL AUTO_INCREMENT,
  `full_date` date NOT NULL,
  `day_since_2005` int DEFAULT NULL,
  `month_since_2005` int DEFAULT NULL,
  `day_of_week` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `calendar_month` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `calendar_year` int DEFAULT NULL,
  `calendar_year_month` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `day_of_month` int DEFAULT NULL,
  `day_of_year` int DEFAULT NULL,
  `week_of_year_sunday` int DEFAULT NULL,
  `year_week_sunday` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `week_sunday_start` date DEFAULT NULL,
  `week_of_year_monday` int DEFAULT NULL,
  `year_week_monday` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `week_monday_start` date DEFAULT NULL,
  `holiday` varchar(15) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `day_type` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`date_sk`),
  UNIQUE KEY `full_date` (`full_date`),
  KEY `idx_full_date` (`full_date`),
  KEY `idx_calendar_year` (`calendar_year`),
  KEY `idx_calendar_year_month` (`calendar_year_month`)
) ENGINE=InnoDB AUTO_INCREMENT=7672 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_daily_product_metrics`
--

DROP TABLE IF EXISTS `fact_daily_product_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_daily_product_metrics` (
  `id` int NOT NULL AUTO_INCREMENT,
  `product_id` int DEFAULT NULL,
  `date_key` int DEFAULT NULL,
  `original_price` decimal(12,2) DEFAULT NULL,
  `discount_price` decimal(12,2) DEFAULT NULL,
  `discount_percent` decimal(5,2) DEFAULT NULL,
  `rating` decimal(3,2) DEFAULT NULL,
  `rating_count` int DEFAULT NULL,
  `sold_count` int DEFAULT NULL,
  `sold_today` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_date_key` (`date_key`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_product_date` (`product_id`,`date_key`),
  CONSTRAINT `fact_daily_product_metrics_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`),
  CONSTRAINT `fact_daily_product_metrics_ibfk_2` FOREIGN KEY (`date_key`) REFERENCES `date_dim` (`date_sk`)
) ENGINE=InnoDB AUTO_INCREMENT=1556 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `price_range_aggregate`
--

DROP TABLE IF EXISTS `price_range_aggregate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `price_range_aggregate` (
  `price_range` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `product_count` int DEFAULT '0',
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`price_range`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `product_author_bridge`
--

DROP TABLE IF EXISTS `product_author_bridge`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `product_author_bridge` (
  `product_id` int NOT NULL,
  `author_id` int NOT NULL,
  PRIMARY KEY (`product_id`,`author_id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_author_id` (`author_id`),
  CONSTRAINT `product_author_bridge_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`) ON DELETE CASCADE,
  CONSTRAINT `product_author_bridge_ibfk_2` FOREIGN KEY (`author_id`) REFERENCES `author_dim` (`author_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `product_category_bridge`
--

DROP TABLE IF EXISTS `product_category_bridge`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `product_category_bridge` (
  `product_id` int NOT NULL,
  `category_id` int NOT NULL,
  PRIMARY KEY (`product_id`,`category_id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_category_id` (`category_id`),
  CONSTRAINT `product_category_bridge_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`) ON DELETE CASCADE,
  CONSTRAINT `product_category_bridge_ibfk_2` FOREIGN KEY (`category_id`) REFERENCES `category_dim` (`category_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `product_dim`
--

DROP TABLE IF EXISTS `product_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `product_dim` (
  `id` int NOT NULL AUTO_INCREMENT,
  `product_name` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `isbn` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `language` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `page_count` int DEFAULT NULL,
  `weight` decimal(8,2) DEFAULT NULL,
  `dimensions` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `thumbnail_url` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url_path` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `short_description` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `is_deleted` tinyint(1) DEFAULT '0',
  `deleted_at` timestamp NULL DEFAULT NULL,
  `action_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_product_name` (`product_name`(100)),
  KEY `idx_isbn` (`isbn`),
  KEY `idx_is_deleted` (`is_deleted`)
) ENGINE=InnoDB AUTO_INCREMENT=694 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `product_publisher_bridge`
--

DROP TABLE IF EXISTS `product_publisher_bridge`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `product_publisher_bridge` (
  `product_id` int NOT NULL,
  `publisher_id` int NOT NULL,
  PRIMARY KEY (`product_id`,`publisher_id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_publisher_id` (`publisher_id`),
  CONSTRAINT `product_publisher_bridge_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`) ON DELETE CASCADE,
  CONSTRAINT `product_publisher_bridge_ibfk_2` FOREIGN KEY (`publisher_id`) REFERENCES `publisher_dim` (`publisher_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `publisher_dim`
--

DROP TABLE IF EXISTS `publisher_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `publisher_dim` (
  `publisher_id` int NOT NULL AUTO_INCREMENT,
  `publisher_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`publisher_id`),
  UNIQUE KEY `uq_publisher_name` (`publisher_name`),
  KEY `idx_publisher_name` (`publisher_name`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `publisher_revenue_aggregate`
--

DROP TABLE IF EXISTS `publisher_revenue_aggregate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `publisher_revenue_aggregate` (
  `publisher_id` int NOT NULL,
  `total_books` int DEFAULT '0',
  `total_sold` int DEFAULT '0',
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `market_share` decimal(5,2) DEFAULT NULL,
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`publisher_id`),
  KEY `idx_total_revenue` (`total_revenue`),
  KEY `idx_market_share` (`market_share`),
  CONSTRAINT `publisher_revenue_aggregate_ibfk_1` FOREIGN KEY (`publisher_id`) REFERENCES `publisher_dim` (`publisher_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rating_aggregate`
--

DROP TABLE IF EXISTS `rating_aggregate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rating_aggregate` (
  `rating_range` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `product_count` int DEFAULT '0',
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`rating_range`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-11-20 17:45:20
