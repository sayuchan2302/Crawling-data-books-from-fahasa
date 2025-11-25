-- MySQL dump 10.13  Distrib 8.0.44, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: fahasa_control
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
-- Table structure for table `config`
--

DROP TABLE IF EXISTS `config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `config` (
  `id` int NOT NULL AUTO_INCREMENT,
  `file_name` varchar(255) DEFAULT NULL,
  `file_path` varchar(255) DEFAULT NULL,
  `file_encoding` varchar(255) DEFAULT NULL,
  `crawl_frequency` int DEFAULT NULL,
  `data_size` int DEFAULT NULL,
  `retry_count` int DEFAULT NULL,
  `timeout` int DEFAULT NULL,
  `dw_source_port` int DEFAULT NULL,
  `staging_source_port` int DEFAULT NULL,
  `source_path` varchar(255) DEFAULT NULL,
  `destination_path` varchar(255) DEFAULT NULL,
  `backup_path` varchar(255) DEFAULT NULL,
  `file_type` enum('csv','json','xml') DEFAULT NULL,
  `delimiter` varchar(255) DEFAULT NULL,
  `dw_source_host` varchar(255) DEFAULT NULL,
  `dw_source_password` varchar(255) DEFAULT NULL,
  `dw_source_username` varchar(255) DEFAULT NULL,
  `staging_source_host` varchar(255) DEFAULT NULL,
  `staging_source_password` varchar(255) DEFAULT NULL,
  `staging_source_username` varchar(255) DEFAULT NULL,
  `columns` longtext,
  `tables` varchar(255) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `notification_emails` varchar(255) DEFAULT NULL,
  `create_time` datetime(6) DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` datetime(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `created_by` varchar(255) DEFAULT NULL,
  `updated_by` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `is_active` bit(1) DEFAULT b'1',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `logs`
--

DROP TABLE IF EXISTS `logs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `logs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_config` int DEFAULT NULL,
  `count` int DEFAULT NULL,
  `log_level` enum('INFO','WARN','ERROR') DEFAULT NULL,
  `status` enum('SUCCESS','FAILED','RUNNING') DEFAULT NULL,
  `create_time` datetime(6) DEFAULT CURRENT_TIMESTAMP(6),
  `update_time` datetime(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  `created_by` varchar(255) DEFAULT NULL,
  `destination_path` varchar(255) DEFAULT NULL,
  `error_message` text,
  `location` varchar(255) DEFAULT NULL,
  `stack_trace` longtext,
  PRIMARY KEY (`id`),
  KEY `idx_id_config` (`id_config`),
  KEY `idx_log_level` (`log_level`),
  KEY `idx_status` (`status`),
  KEY `idx_create_time` (`create_time`),
  CONSTRAINT `fk_logs_config` FOREIGN KEY (`id_config`) REFERENCES `config` (`id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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

-- Dump completed on 2025-11-25 16:19:56
