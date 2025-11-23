-- =============================================
-- Fahasa Data Warehouse Schema
-- Generated on: 2025-11-23 13:13:02
-- Purpose: Star schema data warehouse with fact and dimension tables
-- Architecture: Dimensional modeling for analytics and reporting
-- =============================================

-- Create database
CREATE DATABASE IF NOT EXISTS fahasa_dw;
USE fahasa_dw;

-- Set charset for proper text handling
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- =============================================
-- Dimension Table: dim_books (19 columns, 0 rows)
-- Purpose: Book dimension with detailed attributes and hierarchy
-- Type: Slowly Changing Dimension (SCD)
-- Grain: One row per books instance
-- =============================================
DROP TABLE IF EXISTS `dim_books`;

CREATE TABLE `dim_books` (
  `book_sk` int NOT NULL AUTO_INCREMENT,
  `book_nk` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `author` text COLLATE utf8mb4_unicode_ci,
  `publisher` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `supplier` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publish_year` int DEFAULT NULL,
  `language` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `page_count` int DEFAULT NULL,
  `weight` decimal(8,3) DEFAULT NULL,
  `dimensions` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `url` text COLLATE utf8mb4_unicode_ci,
  `url_img` text COLLATE utf8mb4_unicode_ci,
  `effective_date` date NOT NULL,
  `expiry_date` date DEFAULT '9999-12-31',
  `is_current` tinyint(1) DEFAULT '1',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `source_system` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT 'FAHASA',
  PRIMARY KEY (`book_sk`),
  UNIQUE KEY `uk_book_effective` (`book_nk`,`effective_date`),
  KEY `idx_book_nk` (`book_nk`),
  KEY `idx_is_current` (`is_current`),
  KEY `idx_effective_date` (`effective_date`),
  KEY `idx_publisher` (`publisher`),
  KEY `idx_supplier` (`supplier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for dim_books (19 columns):
-- Keys & Identifiers:
-- book_sk                   int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Measures & Metrics:
-- page_count                int                 
-- Attributes:
-- book_nk                   varchar(255)         -- INDEXED, NOT NULL
-- title                     text                 -- NOT NULL
-- author                    text                
-- publisher                 varchar(255)         -- INDEXED
-- supplier                  varchar(255)         -- INDEXED
-- publish_year              int                 
-- language                  varchar(100)        
-- weight                    decimal(8,3)        
-- dimensions                varchar(100)        
-- url                       text                
-- url_img                   text                
-- effective_date            date                 -- INDEXED, NOT NULL
-- expiry_date               date                 -- DEFAULT '9999-12-31'
-- source_system             varchar(50)          -- DEFAULT 'FAHASA'
-- Audit & Control:
-- is_current                tinyint(1)           -- INDEXED, DEFAULT '1'
-- created_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED
-- updated_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- Indexes for dim_books:
CREATE UNIQUE INDEX `uk_book_effective` ON `dim_books` (`book_nk`, `effective_date`);
CREATE INDEX `idx_book_nk` ON `dim_books` (`book_nk`);
CREATE INDEX `idx_is_current` ON `dim_books` (`is_current`);
CREATE INDEX `idx_effective_date` ON `dim_books` (`effective_date`);
CREATE INDEX `idx_publisher` ON `dim_books` (`publisher`);
CREATE INDEX `idx_supplier` ON `dim_books` (`supplier`);

-- End of dim_books table

-- =============================================
-- Dimension Table: dim_categories (8 columns, 3 rows)
-- Purpose: Category dimension with hierarchical structure
-- Type: Slowly Changing Dimension (SCD)
-- Grain: One row per categories instance
-- =============================================
DROP TABLE IF EXISTS `dim_categories`;

CREATE TABLE `dim_categories` (
  `category_sk` int NOT NULL AUTO_INCREMENT,
  `category_nk` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL,
  `category_level_1` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_level_2` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_level_3` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_path` varchar(1000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`category_sk`),
  UNIQUE KEY `uk_category_nk` (`category_nk`),
  KEY `idx_level_1` (`category_level_1`),
  KEY `idx_level_2` (`category_level_2`),
  KEY `idx_category_path` (`category_path`(255))
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for dim_categories (8 columns):
-- Keys & Identifiers:
-- category_sk               int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Attributes:
-- category_nk               varchar(500)         -- UNIQUE, NOT NULL
-- category_level_1          varchar(255)         -- INDEXED
-- category_level_2          varchar(255)         -- INDEXED
-- category_level_3          varchar(500)        
-- category_path             varchar(1000)        -- INDEXED
-- Audit & Control:
-- created_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED
-- updated_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- Indexes for dim_categories:
CREATE UNIQUE INDEX `uk_category_nk` ON `dim_categories` (`category_nk`);
CREATE INDEX `idx_level_1` ON `dim_categories` (`category_level_1`);
CREATE INDEX `idx_level_2` ON `dim_categories` (`category_level_2`);
CREATE INDEX `idx_category_path` ON `dim_categories` (`category_path`);

-- End of dim_categories table

-- =============================================
-- Dimension Table: dim_date (14 columns, 0 rows)
-- Purpose: Date dimension with calendar attributes
-- Type: Slowly Changing Dimension (SCD)
-- Grain: One row per date instance
-- =============================================
DROP TABLE IF EXISTS `dim_date`;

CREATE TABLE `dim_date` (
  `date_sk` int NOT NULL,
  `full_date` date NOT NULL,
  `year` int NOT NULL,
  `quarter` int NOT NULL,
  `month` int NOT NULL,
  `month_name` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `day` int NOT NULL,
  `day_of_week` int NOT NULL,
  `day_name` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `week_of_year` int NOT NULL,
  `is_weekend` tinyint(1) DEFAULT '0',
  `is_holiday` tinyint(1) DEFAULT '0',
  `fiscal_year` int DEFAULT NULL,
  `fiscal_quarter` int DEFAULT NULL,
  PRIMARY KEY (`date_sk`),
  UNIQUE KEY `uk_full_date` (`full_date`),
  KEY `idx_year` (`year`),
  KEY `idx_quarter` (`quarter`),
  KEY `idx_month` (`month`),
  KEY `idx_is_weekend` (`is_weekend`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for dim_date (14 columns):
-- Keys & Identifiers:
-- date_sk                   int                  -- PRIMARY KEY, NOT NULL
-- is_holiday                tinyint(1)           -- DEFAULT '0'
-- Attributes:
-- full_date                 date                 -- UNIQUE, NOT NULL
-- year                      int                  -- INDEXED, NOT NULL
-- quarter                   int                  -- INDEXED, NOT NULL
-- month                     int                  -- INDEXED, NOT NULL
-- month_name                varchar(20)         
-- day                       int                  -- NOT NULL
-- day_of_week               int                  -- NOT NULL
-- day_name                  varchar(20)         
-- week_of_year              int                  -- NOT NULL
-- is_weekend                tinyint(1)           -- INDEXED, DEFAULT '0'
-- fiscal_year               int                 
-- fiscal_quarter            int                 

-- Indexes for dim_date:
CREATE UNIQUE INDEX `uk_full_date` ON `dim_date` (`full_date`);
CREATE INDEX `idx_year` ON `dim_date` (`year`);
CREATE INDEX `idx_quarter` ON `dim_date` (`quarter`);
CREATE INDEX `idx_month` ON `dim_date` (`month`);
CREATE INDEX `idx_is_weekend` ON `dim_date` (`is_weekend`);

-- Sample date queries:
-- SELECT * FROM dim_date WHERE year = 2024 AND quarter = 1;
-- SELECT year, quarter, COUNT(*) FROM dim_date GROUP BY year, quarter;

-- End of dim_date table

-- =============================================
-- Fact Table: fact_book_sales (15 columns, 0 rows)
-- Purpose: Book_Sales fact table
-- Type: Transaction/Snapshot Fact Table
-- Grain: One row per book_sales event
-- =============================================
DROP TABLE IF EXISTS `fact_book_sales`;

CREATE TABLE `fact_book_sales` (
  `sales_sk` bigint NOT NULL AUTO_INCREMENT,
  `book_sk` int NOT NULL,
  `category_sk` int NOT NULL,
  `date_sk` int NOT NULL,
  `original_price` decimal(15,2) DEFAULT NULL,
  `discount_price` decimal(15,2) DEFAULT NULL,
  `discount_amount` decimal(15,2) DEFAULT NULL,
  `discount_percent` decimal(5,2) DEFAULT NULL,
  `rating` decimal(3,2) DEFAULT NULL,
  `rating_count` int DEFAULT '0',
  `sold_count_numeric` int DEFAULT '0',
  `revenue_potential` decimal(18,2) DEFAULT NULL,
  `discount_total` decimal(18,2) DEFAULT NULL,
  `batch_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`sales_sk`),
  KEY `idx_book_sk` (`book_sk`),
  KEY `idx_category_sk` (`category_sk`),
  KEY `idx_date_sk` (`date_sk`),
  KEY `idx_batch_id` (`batch_id`),
  KEY `idx_created_at` (`created_at`),
  CONSTRAINT `fact_book_sales_ibfk_1` FOREIGN KEY (`book_sk`) REFERENCES `dim_books` (`book_sk`),
  CONSTRAINT `fact_book_sales_ibfk_2` FOREIGN KEY (`category_sk`) REFERENCES `dim_categories` (`category_sk`),
  CONSTRAINT `fact_book_sales_ibfk_3` FOREIGN KEY (`date_sk`) REFERENCES `dim_date` (`date_sk`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for fact_book_sales (15 columns):
-- Keys & Identifiers:
-- sales_sk                  bigint               -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- batch_id                  int                  -- INDEXED
-- Measures & Metrics:
-- original_price            decimal(15,2)       
-- discount_price            decimal(15,2)       
-- discount_amount           decimal(15,2)       
-- discount_percent          decimal(5,2)        
-- rating_count              int                  -- DEFAULT '0'
-- sold_count_numeric        int                  -- DEFAULT '0'
-- discount_total            decimal(18,2)       
-- Attributes:
-- book_sk                   int                  -- INDEXED, NOT NULL
-- category_sk               int                  -- INDEXED, NOT NULL
-- date_sk                   int                  -- INDEXED, NOT NULL
-- rating                    decimal(3,2)        
-- revenue_potential         decimal(18,2)       
-- Audit & Control:
-- created_at                timestamp            -- INDEXED, DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED

-- Indexes for fact_book_sales:
CREATE INDEX `idx_book_sk` ON `fact_book_sales` (`book_sk`);
CREATE INDEX `idx_category_sk` ON `fact_book_sales` (`category_sk`);
CREATE INDEX `idx_date_sk` ON `fact_book_sales` (`date_sk`);
CREATE INDEX `idx_batch_id` ON `fact_book_sales` (`batch_id`);
CREATE INDEX `idx_created_at` ON `fact_book_sales` (`created_at`);

-- Sample analytical queries for fact_book_sales:
-- SELECT d.year, d.month, SUM(f.amount) FROM fact_book_sales f
-- JOIN dim_date d ON f.date_id = d.date_id
-- GROUP BY d.year, d.month;

-- End of fact_book_sales table

-- =============================================
-- Fact Table: fact_daily_product_metrics (11 columns, 318 rows)
-- Purpose: Daily_Product_Metrics fact table
-- Type: Transaction/Snapshot Fact Table
-- Grain: One row per daily_product_metrics event
-- =============================================
DROP TABLE IF EXISTS `fact_daily_product_metrics`;

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

-- Column Details for fact_daily_product_metrics (11 columns):
-- Keys & Identifiers:
-- id                        int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- product_id                int                  -- INDEXED
-- Measures & Metrics:
-- original_price            decimal(12,2)       
-- discount_price            decimal(12,2)       
-- discount_percent          decimal(5,2)        
-- rating_count              int                 
-- sold_count                int                 
-- Attributes:
-- date_key                  int                  -- INDEXED
-- rating                    decimal(3,2)        
-- sold_today                int                 
-- Audit & Control:
-- created_at                timestamp            -- INDEXED, DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED

-- Indexes for fact_daily_product_metrics:
CREATE INDEX `idx_product_id` ON `fact_daily_product_metrics` (`product_id`);
CREATE INDEX `idx_date_key` ON `fact_daily_product_metrics` (`date_key`);
CREATE INDEX `idx_created_at` ON `fact_daily_product_metrics` (`created_at`);
CREATE INDEX `idx_product_date` ON `fact_daily_product_metrics` (`product_id`, `date_key`);

-- Sample analytical queries for fact_daily_product_metrics:
-- SELECT d.year, d.month, SUM(f.amount) FROM fact_daily_product_metrics f
-- JOIN dim_date d ON f.date_id = d.date_id
-- GROUP BY d.year, d.month;

-- End of fact_daily_product_metrics table

-- =============================================
-- Other Table: author_dim (2 columns, 19 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `author_dim`;

CREATE TABLE `author_dim` (
  `author_id` int NOT NULL AUTO_INCREMENT,
  `author_name` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`author_id`),
  UNIQUE KEY `uq_author_name` (`author_name`),
  KEY `idx_author_name` (`author_name`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for author_dim (2 columns):
-- Keys & Identifiers:
-- author_id                 int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Attributes:
-- author_name               varchar(300)         -- UNIQUE, NOT NULL

-- Indexes for author_dim:
CREATE UNIQUE INDEX `uq_author_name` ON `author_dim` (`author_name`);
CREATE INDEX `idx_author_name` ON `author_dim` (`author_name`);

-- End of author_dim table

-- =============================================
-- Other Table: author_performance_aggregate (7 columns, 19 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `author_performance_aggregate`;

CREATE TABLE `author_performance_aggregate` (
  `author_id` int NOT NULL,
  `author_name` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
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

-- Column Details for author_performance_aggregate (7 columns):
-- Keys & Identifiers:
-- author_id                 int                  -- PRIMARY KEY, NOT NULL
-- Measures & Metrics:
-- total_books               int                  -- INDEXED, DEFAULT '0'
-- avg_rating                decimal(3,2)         -- INDEXED
-- total_revenue             decimal(15,2)        -- INDEXED
-- total_sold                int                  -- DEFAULT '0'
-- Attributes:
-- author_name               varchar(300)        
-- last_updated              timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- Indexes for author_performance_aggregate:
CREATE INDEX `idx_total_books` ON `author_performance_aggregate` (`total_books`);
CREATE INDEX `idx_avg_rating` ON `author_performance_aggregate` (`avg_rating`);
CREATE INDEX `idx_total_revenue` ON `author_performance_aggregate` (`total_revenue`);

-- End of author_performance_aggregate table

-- =============================================
-- Other Table: bridge_book_categories (8 columns, 0 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `bridge_book_categories`;

CREATE TABLE `bridge_book_categories` (
  `bridge_sk` bigint NOT NULL AUTO_INCREMENT,
  `book_sk` int NOT NULL,
  `category_sk` int NOT NULL,
  `allocation_factor` decimal(5,4) DEFAULT '1.0000',
  `effective_date` date NOT NULL,
  `expiry_date` date DEFAULT '9999-12-31',
  `is_current` tinyint(1) DEFAULT '1',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`bridge_sk`),
  UNIQUE KEY `uk_book_category_effective` (`book_sk`,`category_sk`,`effective_date`),
  KEY `idx_book_sk` (`book_sk`),
  KEY `idx_category_sk` (`category_sk`),
  KEY `idx_is_current` (`is_current`),
  CONSTRAINT `bridge_book_categories_ibfk_1` FOREIGN KEY (`book_sk`) REFERENCES `dim_books` (`book_sk`),
  CONSTRAINT `bridge_book_categories_ibfk_2` FOREIGN KEY (`category_sk`) REFERENCES `dim_categories` (`category_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for bridge_book_categories (8 columns):
-- Keys & Identifiers:
-- bridge_sk                 bigint               -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Attributes:
-- book_sk                   int                  -- INDEXED, NOT NULL
-- category_sk               int                  -- INDEXED, NOT NULL
-- allocation_factor         decimal(5,4)         -- DEFAULT '1.0000'
-- effective_date            date                 -- NOT NULL
-- expiry_date               date                 -- DEFAULT '9999-12-31'
-- Audit & Control:
-- is_current                tinyint(1)           -- INDEXED, DEFAULT '1'
-- created_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED

-- Indexes for bridge_book_categories:
CREATE UNIQUE INDEX `uk_book_category_effective` ON `bridge_book_categories` (`book_sk`, `category_sk`, `effective_date`);
CREATE INDEX `idx_book_sk` ON `bridge_book_categories` (`book_sk`);
CREATE INDEX `idx_category_sk` ON `bridge_book_categories` (`category_sk`);
CREATE INDEX `idx_is_current` ON `bridge_book_categories` (`is_current`);

-- End of bridge_book_categories table

-- =============================================
-- Other Table: category_dim (5 columns, 196 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `category_dim`;

CREATE TABLE `category_dim` (
  `category_id` int NOT NULL AUTO_INCREMENT,
  `level_1` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `level_2` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `level_3` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_path` varchar(300) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`category_id`),
  KEY `idx_level_1` (`level_1`),
  KEY `idx_category_path` (`category_path`(100))
) ENGINE=InnoDB AUTO_INCREMENT=287 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for category_dim (5 columns):
-- Keys & Identifiers:
-- category_id               int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Attributes:
-- level_1                   varchar(100)         -- INDEXED
-- level_2                   varchar(100)        
-- level_3                   varchar(100)        
-- category_path             varchar(300)         -- INDEXED

-- Indexes for category_dim:
CREATE INDEX `idx_level_1` ON `category_dim` (`level_1`);
CREATE INDEX `idx_category_path` ON `category_dim` (`category_path`);

-- End of category_dim table

-- =============================================
-- Other Table: category_sales_aggregate (8 columns, 196 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `category_sales_aggregate`;

CREATE TABLE `category_sales_aggregate` (
  `category_id` int NOT NULL,
  `category_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
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

-- Column Details for category_sales_aggregate (8 columns):
-- Keys & Identifiers:
-- category_id               int                  -- PRIMARY KEY, NOT NULL
-- Measures & Metrics:
-- total_books               int                  -- INDEXED, DEFAULT '0'
-- avg_price                 decimal(12,2)       
-- avg_rating                decimal(3,2)        
-- total_sold                int                  -- DEFAULT '0'
-- Attributes:
-- category_name             varchar(100)        
-- market_share              decimal(5,2)         -- INDEXED
-- last_updated              timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- Indexes for category_sales_aggregate:
CREATE INDEX `idx_total_books` ON `category_sales_aggregate` (`total_books`);
CREATE INDEX `idx_market_share` ON `category_sales_aggregate` (`market_share`);

-- End of category_sales_aggregate table

-- =============================================
-- Other Table: date_dim (18 columns, 7,670 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `date_dim`;

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

-- Column Details for date_dim (18 columns):
-- Keys & Identifiers:
-- date_sk                   int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- holiday                   varchar(15)         
-- Attributes:
-- full_date                 date                 -- UNIQUE, NOT NULL
-- day_since_2005            int                 
-- month_since_2005          int                 
-- day_of_week               varchar(10)         
-- calendar_month            varchar(10)         
-- calendar_year             int                  -- INDEXED
-- calendar_year_month       varchar(10)          -- INDEXED
-- day_of_month              int                 
-- day_of_year               int                 
-- week_of_year_sunday       int                 
-- year_week_sunday          varchar(10)         
-- week_sunday_start         date                
-- week_of_year_monday       int                 
-- year_week_monday          varchar(10)         
-- week_monday_start         date                
-- day_type                  varchar(10)         

-- Indexes for date_dim:
CREATE UNIQUE INDEX `full_date` ON `date_dim` (`full_date`);
CREATE INDEX `idx_full_date` ON `date_dim` (`full_date`);
CREATE INDEX `idx_calendar_year` ON `date_dim` (`calendar_year`);
CREATE INDEX `idx_calendar_year_month` ON `date_dim` (`calendar_year_month`);

-- End of date_dim table

-- =============================================
-- Other Table: price_range_aggregate (4 columns, 0 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `price_range_aggregate`;

CREATE TABLE `price_range_aggregate` (
  `price_range` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total_books` int DEFAULT '0',
  `product_count` int DEFAULT '0',
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`price_range`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for price_range_aggregate (4 columns):
-- Keys & Identifiers:
-- price_range               varchar(50)          -- PRIMARY KEY, NOT NULL
-- Measures & Metrics:
-- total_books               int                  -- DEFAULT '0'
-- product_count             int                  -- DEFAULT '0'
-- Attributes:
-- last_updated              timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- End of price_range_aggregate table

-- =============================================
-- Other Table: product_author_bridge (2 columns, 782 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `product_author_bridge`;

CREATE TABLE `product_author_bridge` (
  `product_id` int NOT NULL,
  `author_id` int NOT NULL,
  PRIMARY KEY (`product_id`,`author_id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_author_id` (`author_id`),
  CONSTRAINT `product_author_bridge_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`) ON DELETE CASCADE,
  CONSTRAINT `product_author_bridge_ibfk_2` FOREIGN KEY (`author_id`) REFERENCES `author_dim` (`author_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for product_author_bridge (2 columns):
-- Keys & Identifiers:
-- product_id                int                  -- PRIMARY KEY, NOT NULL
-- author_id                 int                  -- PRIMARY KEY, NOT NULL

-- Indexes for product_author_bridge:
CREATE INDEX `idx_product_id` ON `product_author_bridge` (`product_id`);
CREATE INDEX `idx_author_id` ON `product_author_bridge` (`author_id`);

-- End of product_author_bridge table

-- =============================================
-- Other Table: product_category_bridge (2 columns, 150,332 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `product_category_bridge`;

CREATE TABLE `product_category_bridge` (
  `product_id` int NOT NULL,
  `category_id` int NOT NULL,
  PRIMARY KEY (`product_id`,`category_id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_category_id` (`category_id`),
  CONSTRAINT `product_category_bridge_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`) ON DELETE CASCADE,
  CONSTRAINT `product_category_bridge_ibfk_2` FOREIGN KEY (`category_id`) REFERENCES `category_dim` (`category_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for product_category_bridge (2 columns):
-- Keys & Identifiers:
-- product_id                int                  -- PRIMARY KEY, NOT NULL
-- category_id               int                  -- PRIMARY KEY, NOT NULL

-- Indexes for product_category_bridge:
CREATE INDEX `idx_product_id` ON `product_category_bridge` (`product_id`);
CREATE INDEX `idx_category_id` ON `product_category_bridge` (`category_id`);

-- End of product_category_bridge table

-- =============================================
-- Other Table: product_dim (14 columns, 782 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `product_dim`;

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
) ENGINE=InnoDB AUTO_INCREMENT=1261 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for product_dim (14 columns):
-- Keys & Identifiers:
-- id                        int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Measures & Metrics:
-- page_count                int                 
-- Attributes:
-- product_name              varchar(500)         -- INDEXED
-- isbn                      varchar(50)          -- INDEXED
-- language                  varchar(50)         
-- weight                    decimal(8,2)        
-- dimensions                varchar(100)        
-- thumbnail_url             varchar(500)        
-- url_path                  varchar(500)        
-- short_description         text                
-- is_deleted                tinyint(1)           -- INDEXED, DEFAULT '0'
-- deleted_at                timestamp           
-- action_time               timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP
-- Audit & Control:
-- created_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED

-- Indexes for product_dim:
CREATE INDEX `idx_product_name` ON `product_dim` (`product_name`);
CREATE INDEX `idx_isbn` ON `product_dim` (`isbn`);
CREATE INDEX `idx_is_deleted` ON `product_dim` (`is_deleted`);

-- End of product_dim table

-- =============================================
-- Other Table: product_publisher_bridge (2 columns, 767 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `product_publisher_bridge`;

CREATE TABLE `product_publisher_bridge` (
  `product_id` int NOT NULL,
  `publisher_id` int NOT NULL,
  PRIMARY KEY (`product_id`,`publisher_id`),
  KEY `idx_product_id` (`product_id`),
  KEY `idx_publisher_id` (`publisher_id`),
  CONSTRAINT `product_publisher_bridge_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product_dim` (`id`) ON DELETE CASCADE,
  CONSTRAINT `product_publisher_bridge_ibfk_2` FOREIGN KEY (`publisher_id`) REFERENCES `publisher_dim` (`publisher_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for product_publisher_bridge (2 columns):
-- Keys & Identifiers:
-- product_id                int                  -- PRIMARY KEY, NOT NULL
-- publisher_id              int                  -- PRIMARY KEY, NOT NULL

-- Indexes for product_publisher_bridge:
CREATE INDEX `idx_product_id` ON `product_publisher_bridge` (`product_id`);
CREATE INDEX `idx_publisher_id` ON `product_publisher_bridge` (`publisher_id`);

-- End of product_publisher_bridge table

-- =============================================
-- Other Table: publisher_dim (2 columns, 9 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `publisher_dim`;

CREATE TABLE `publisher_dim` (
  `publisher_id` int NOT NULL AUTO_INCREMENT,
  `publisher_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`publisher_id`),
  UNIQUE KEY `uq_publisher_name` (`publisher_name`),
  KEY `idx_publisher_name` (`publisher_name`)
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for publisher_dim (2 columns):
-- Keys & Identifiers:
-- publisher_id              int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- Attributes:
-- publisher_name            varchar(200)         -- UNIQUE, NOT NULL

-- Indexes for publisher_dim:
CREATE UNIQUE INDEX `uq_publisher_name` ON `publisher_dim` (`publisher_name`);
CREATE INDEX `idx_publisher_name` ON `publisher_dim` (`publisher_name`);

-- End of publisher_dim table

-- =============================================
-- Other Table: publisher_revenue_aggregate (7 columns, 9 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `publisher_revenue_aggregate`;

CREATE TABLE `publisher_revenue_aggregate` (
  `publisher_id` int NOT NULL,
  `publisher_name` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
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

-- Column Details for publisher_revenue_aggregate (7 columns):
-- Keys & Identifiers:
-- publisher_id              int                  -- PRIMARY KEY, NOT NULL
-- Measures & Metrics:
-- total_books               int                  -- DEFAULT '0'
-- total_sold                int                  -- DEFAULT '0'
-- total_revenue             decimal(15,2)        -- INDEXED
-- Attributes:
-- publisher_name            varchar(200)        
-- market_share              decimal(5,2)         -- INDEXED
-- last_updated              timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- Indexes for publisher_revenue_aggregate:
CREATE INDEX `idx_total_revenue` ON `publisher_revenue_aggregate` (`total_revenue`);
CREATE INDEX `idx_market_share` ON `publisher_revenue_aggregate` (`market_share`);

-- End of publisher_revenue_aggregate table

-- =============================================
-- Other Table: rating_aggregate (4 columns, 0 rows)
-- Purpose: Data warehouse table
-- =============================================
DROP TABLE IF EXISTS `rating_aggregate`;

CREATE TABLE `rating_aggregate` (
  `rating_range` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total_books` int DEFAULT '0',
  `product_count` int DEFAULT '0',
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`rating_range`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for rating_aggregate (4 columns):
-- Keys & Identifiers:
-- rating_range              varchar(50)          -- PRIMARY KEY, NOT NULL
-- Measures & Metrics:
-- total_books               int                  -- DEFAULT '0'
-- product_count             int                  -- DEFAULT '0'
-- Attributes:
-- last_updated              timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP

-- End of rating_aggregate table

-- =============================================
-- Data Warehouse Architecture Summary
-- =============================================

-- Fahasa Data Warehouse follows a star schema design:
-- 
-- Dimension Tables (Master Data):
-- - dim_authors: Author master data with biographical information
-- - dim_books: Book catalog with detailed attributes and classifications
-- - dim_categories: Hierarchical category structure for book classification
-- - dim_date: Calendar dimension for time-based analysis
-- - dim_publishers: Publisher information and company details
-- - dim_suppliers: Supplier master data for supply chain analytics
--
-- Fact Tables (Transactional Data):
-- - fact_sales: Sales transaction facts with revenue and quantity measures
-- - fact_inventory: Inventory snapshot facts for stock level analysis
-- - fact_book_metrics: Book performance metrics and KPIs
--
-- Key Design Principles:
-- 1. Star Schema: Central fact tables surrounded by dimension tables
-- 2. Slowly Changing Dimensions: Historical tracking of dimension changes
-- 3. Surrogate Keys: Integer keys for optimal join performance
-- 4. Audit Columns: Created/updated timestamps for data lineage
-- 5. Denormalized Design: Optimized for analytical queries

-- =============================================
-- Common Analytics Patterns
-- =============================================

-- Sales Analysis by Time:
-- SELECT 
--     d.year,
--     d.month_name,
--     SUM(fs.revenue_amount) as total_revenue,
--     COUNT(fs.transaction_id) as transaction_count
-- FROM fact_sales fs
-- JOIN dim_date d ON fs.date_id = d.date_id
-- GROUP BY d.year, d.month_num, d.month_name
-- ORDER BY d.year, d.month_num;

-- Top Selling Books:
-- SELECT 
--     b.title,
--     b.author_name,
--     SUM(fs.quantity_sold) as total_quantity,
--     SUM(fs.revenue_amount) as total_revenue
-- FROM fact_sales fs
-- JOIN dim_books b ON fs.book_id = b.book_id
-- GROUP BY b.book_id, b.title, b.author_name
-- ORDER BY total_revenue DESC
-- LIMIT 10;

-- Category Performance:
-- SELECT 
--     c.category_name,
--     c.category_level,
--     COUNT(DISTINCT fs.book_id) as unique_books,
--     SUM(fs.quantity_sold) as total_quantity,
--     AVG(fs.unit_price) as avg_price
-- FROM fact_sales fs
-- JOIN dim_books b ON fs.book_id = b.book_id
-- JOIN dim_categories c ON b.category_id = c.category_id
-- GROUP BY c.category_id, c.category_name, c.category_level
-- ORDER BY total_quantity DESC;

-- =============================================
-- Data Refresh and ETL Notes
-- =============================================

-- Incremental Load Strategy:
-- 1. Dimensions: SCD Type 2 for historical tracking
-- 2. Facts: Delta loads based on transaction date
-- 3. Date Dimension: Pre-populated for future dates
-- 4. Control Tables: Track last load timestamps

-- ETL Process Order:
-- 1. Load dimension tables (maintain referential integrity)
-- 2. Load fact tables with proper dimension key lookups
-- 3. Update control logs and audit information
-- 4. Validate data quality and business rules

-- =============================================
-- Re-enable foreign key checks
-- =============================================
SET FOREIGN_KEY_CHECKS = 1;

-- =============================================
-- Post-Creation Verification Queries
-- =============================================

-- Verify all tables created:
-- SHOW TABLES;

-- Check dimension table row counts:
-- SELECT 'dim_authors' as table_name, COUNT(*) as row_count FROM dim_authors
-- UNION ALL
-- SELECT 'dim_books', COUNT(*) FROM dim_books
-- UNION ALL
-- SELECT 'dim_categories', COUNT(*) FROM dim_categories
-- UNION ALL
-- SELECT 'dim_date', COUNT(*) FROM dim_date
-- UNION ALL
-- SELECT 'dim_publishers', COUNT(*) FROM dim_publishers
-- UNION ALL
-- SELECT 'dim_suppliers', COUNT(*) FROM dim_suppliers;

-- Check fact table row counts:
-- SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
-- UNION ALL
-- SELECT 'fact_inventory', COUNT(*) FROM fact_inventory
-- UNION ALL
-- SELECT 'fact_book_metrics', COUNT(*) FROM fact_book_metrics;

-- Verify referential integrity:
-- SELECT COUNT(*) as orphan_records
-- FROM fact_sales fs
-- LEFT JOIN dim_books b ON fs.book_id = b.book_id
-- WHERE b.book_id IS NULL;

-- =============================================
-- End of Fahasa Data Warehouse Schema
-- =============================================
