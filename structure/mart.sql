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

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'a78e9c2b-c485-11f0-9e54-00155d63397d:1-11659';

--
-- Table structure for table `mart_author_insights`
--

DROP TABLE IF EXISTS `mart_author_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_author_insights` (
  `author_id` int NOT NULL,
  `author_name` varchar(300) DEFAULT NULL,
  `total_books` int DEFAULT NULL,
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `most_popular_book` varchar(500) DEFAULT NULL,
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`author_id`),
  KEY `idx_author_name` (`author_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_category_performance`
--

DROP TABLE IF EXISTS `mart_category_performance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_category_performance` (
  `category_id` int NOT NULL,
  `category_path` varchar(300) DEFAULT NULL,
  `total_books` int DEFAULT NULL,
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `avg_price` decimal(12,2) DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `market_share` decimal(5,2) DEFAULT NULL,
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`category_id`),
  KEY `idx_category_path` (`category_path`(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
  `product_id` int NOT NULL,
  `product_name` varchar(500) DEFAULT NULL,
  `category_path` varchar(300) DEFAULT NULL,
  `publisher_name` varchar(200) DEFAULT NULL,
  `author_names` text,
  `price` decimal(12,2) DEFAULT NULL,
  `discount_price` decimal(12,2) DEFAULT NULL,
  `discount_percent` decimal(5,2) DEFAULT NULL,
  `rating` decimal(3,2) DEFAULT NULL,
  `rating_count` int DEFAULT NULL,
  `sold_today` int DEFAULT NULL,
  `sold_cumulative` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_date` (`date`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_category` (`category_path`(100))
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_product_flat`
--

DROP TABLE IF EXISTS `mart_product_flat`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_product_flat` (
  `product_id` int NOT NULL,
  `product_name` varchar(500) DEFAULT NULL,
  `isbn` varchar(50) DEFAULT NULL,
  `category_path` varchar(300) DEFAULT NULL,
  `author_names` text,
  `publisher_name` varchar(200) DEFAULT NULL,
  `page_count` int DEFAULT NULL,
  `weight` decimal(8,2) DEFAULT NULL,
  `dimensions` varchar(100) DEFAULT NULL,
  `language` varchar(50) DEFAULT NULL,
  `first_seen` date DEFAULT NULL,
  `last_seen` date DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `avg_price` decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (`product_id`),
  KEY `idx_product_name` (`product_name`(100)),
  KEY `idx_category_path` (`category_path`(100)),
  KEY `idx_publisher_name` (`publisher_name`),
  KEY `idx_language` (`language`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `mart_publisher_performance`
--

DROP TABLE IF EXISTS `mart_publisher_performance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `mart_publisher_performance` (
  `publisher_id` int NOT NULL,
  `publisher_name` varchar(200) DEFAULT NULL,
  `total_books` int DEFAULT NULL,
  `avg_rating` decimal(3,2) DEFAULT NULL,
  `total_sold` int DEFAULT NULL,
  `total_revenue` decimal(15,2) DEFAULT NULL,
  `market_share` decimal(5,2) DEFAULT NULL,
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`publisher_id`),
  KEY `idx_publisher_name` (`publisher_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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

-- Dump completed on 2025-11-20 17:45:59
