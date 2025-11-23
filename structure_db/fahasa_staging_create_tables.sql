-- =============================================
-- Fahasa Staging Database Schema
-- Generated on: 2025-11-23 12:55:28
-- Purpose: Raw data staging area for Fahasa book data
-- =============================================

-- Create database
CREATE DATABASE IF NOT EXISTS fahasa_staging;
USE fahasa_staging;

-- Set charset for proper Vietnamese text handling
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- =============================================
-- Table: staging_books
-- =============================================
DROP TABLE IF EXISTS `staging_books`;

CREATE TABLE `staging_books` (
  `id` int NOT NULL AUTO_INCREMENT,
  `title` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `author` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publisher` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `supplier` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_1` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_2` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_3` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `original_price` decimal(12,2) DEFAULT NULL,
  `discount_price` decimal(12,2) DEFAULT NULL,
  `discount_percent` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `rating` decimal(3,2) DEFAULT NULL,
  `rating_count` int DEFAULT NULL,
  `sold_count` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sold_count_numeric` int DEFAULT NULL,
  `publish_year` int DEFAULT NULL,
  `language` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `page_count` int DEFAULT NULL,
  `weight` decimal(8,2) DEFAULT NULL,
  `dimensions` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url` text COLLATE utf8mb4_unicode_ci,
  `url_img` text COLLATE utf8mb4_unicode_ci,
  `time_collect` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_staging_books_time_collect` (`time_collect`),
  KEY `idx_staging_books_author` (`author`),
  KEY `idx_staging_books_category1` (`category_1`),
  KEY `idx_staging_books_price` (`original_price`)
) ENGINE=InnoDB AUTO_INCREMENT=188 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Columns in staging_books:
-- id: int (PRIMARY KEY) NOT NULL auto_increment
-- title: varchar(500)
-- author: varchar(300) (INDEXED)
-- publisher: varchar(200)
-- supplier: varchar(200)
-- category_1: varchar(100) (INDEXED)
-- category_2: varchar(100)
-- category_3: varchar(100)
-- original_price: decimal(12,2) (INDEXED)
-- discount_price: decimal(12,2)
-- discount_percent: varchar(20)
-- rating: decimal(3,2)
-- rating_count: int
-- sold_count: varchar(50)
-- sold_count_numeric: int
-- publish_year: int
-- language: varchar(50)
-- page_count: int
-- weight: decimal(8,2)
-- dimensions: varchar(100)
-- url: text
-- url_img: text
-- time_collect: timestamp (INDEXED) DEFAULT CURRENT_TIMESTAMP DEFAULT_GENERATED

-- Indexes for staging_books:
CREATE INDEX `idx_staging_books_time_collect` ON `staging_books` (`time_collect`);
CREATE INDEX `idx_staging_books_author` ON `staging_books` (`author`);
CREATE INDEX `idx_staging_books_category1` ON `staging_books` (`category_1`);
CREATE INDEX `idx_staging_books_price` ON `staging_books` (`original_price`);

-- =============================================
-- Re-enable foreign key checks
-- =============================================
SET FOREIGN_KEY_CHECKS = 1;

-- =============================================
-- End of Schema
-- =============================================
