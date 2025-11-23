-- =============================================
-- Fahasa DataMart Schema
-- Generated on: 2025-11-23
-- Purpose: Business Intelligence and Analytics Mart
-- Architecture: Aggregated tables for reporting and dashboards
-- =============================================

-- Create database
CREATE DATABASE IF NOT EXISTS fahasa_datamart;
USE fahasa_datamart;

-- Set charset for proper text handling
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- =============================================
-- Mart Table: mart_author_insights
-- Purpose: Author performance metrics and insights
-- Grain: One row per author
-- Update frequency: Daily via ETL
-- =============================================
DROP TABLE IF EXISTS mart_author_insights;
CREATE TABLE mart_author_insights (
    author_id INT NOT NULL,
    author_name VARCHAR(300) NOT NULL,
    total_books INT NOT NULL DEFAULT 0,
    avg_rating DECIMAL(3,2) NULL,
    total_sold INT NULL,
    total_revenue DECIMAL(15,2) NULL,
    most_popular_book VARCHAR(500) NULL,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (author_id),
    INDEX idx_author_name (author_name),
    INDEX idx_avg_rating (avg_rating),
    INDEX idx_total_revenue (total_revenue),
    INDEX idx_last_updated (last_updated)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Author performance insights and metrics for BI reporting';

-- =============================================
-- Mart Table: mart_category_performance  
-- Purpose: Category sales performance and trends
-- Grain: One row per category
-- Update frequency: Daily via ETL
-- =============================================
DROP TABLE IF EXISTS mart_category_performance;
CREATE TABLE mart_category_performance (
    category_id INT NOT NULL,
    category_path VARCHAR(300) NOT NULL,
    total_books INT NOT NULL DEFAULT 0,
    avg_rating DECIMAL(3,2) NULL,
    avg_price DECIMAL(12,2) NULL,
    total_sold INT NULL,
    total_revenue DECIMAL(15,2) NULL,
    market_share DECIMAL(5,2) NULL,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (category_id),
    INDEX idx_category_path (category_path),
    INDEX idx_total_revenue (total_revenue),
    INDEX idx_market_share (market_share),
    INDEX idx_last_updated (last_updated)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Category performance metrics for business analysis';

-- =============================================
-- Mart Table: mart_daily_sales
-- Purpose: Daily sales metrics and KPIs
-- Grain: One row per date per product/category combination
-- Update frequency: Daily via ETL
-- =============================================
DROP TABLE IF EXISTS mart_daily_sales;
CREATE TABLE mart_daily_sales (
    id INT NOT NULL AUTO_INCREMENT,
    date DATE NOT NULL,
    product_id INT NULL,
    product_name VARCHAR(500) NULL,
    category_path VARCHAR(300) NULL,
    publisher_name VARCHAR(200) NULL,
    author_names TEXT NULL,
    price DECIMAL(12,2) NULL,
    discount_price DECIMAL(12,2) NULL,
    discount_percent DECIMAL(5,2) NULL,
    rating DECIMAL(3,2) NULL,
    rating_count INT NULL,
    sold_today INT NULL,
    sold_cumulative INT NULL,
    
    PRIMARY KEY (id),
    UNIQUE KEY uk_date_product (date, product_id),
    INDEX idx_date (date),
    INDEX idx_product_id (product_id),
    INDEX idx_category_path (category_path),
    INDEX idx_publisher_name (publisher_name),
    INDEX idx_price (price),
    INDEX idx_rating (rating),
    INDEX idx_sold_today (sold_today)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Daily sales tracking for operational reporting and trend analysis';

-- =============================================
-- Mart Table: mart_product_flat
-- Purpose: Denormalized product view for easy reporting
-- Grain: One row per product with all related attributes
-- Update frequency: Daily via ETL
-- =============================================
DROP TABLE IF EXISTS mart_product_flat;
CREATE TABLE mart_product_flat (
    product_id INT NOT NULL,
    product_name VARCHAR(500) NOT NULL,
    isbn VARCHAR(50) NULL,
    category_path VARCHAR(300) NULL,
    author_names TEXT NULL,
    publisher_name VARCHAR(200) NULL,
    page_count INT NULL,
    weight DECIMAL(8,2) NULL,
    dimensions VARCHAR(100) NULL,
    language VARCHAR(50) NULL,
    first_seen DATE NULL,
    last_seen DATE NULL,
    total_sold INT NULL,
    avg_rating DECIMAL(3,2) NULL,
    avg_price DECIMAL(12,2) NULL,
    
    PRIMARY KEY (product_id),
    INDEX idx_product_name (product_name(100)),
    INDEX idx_isbn (isbn),
    INDEX idx_category_path (category_path),
    INDEX idx_publisher_name (publisher_name),
    INDEX idx_avg_rating (avg_rating),
    INDEX idx_total_sold (total_sold),
    INDEX idx_first_seen (first_seen),
    INDEX idx_last_seen (last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Flattened product view with all attributes for easy reporting and analysis';

-- =============================================
-- Mart Table: mart_publisher_performance
-- Purpose: Publisher business metrics and performance
-- Grain: One row per publisher
-- Update frequency: Daily via ETL
-- =============================================
DROP TABLE IF EXISTS mart_publisher_performance;
CREATE TABLE mart_publisher_performance (
    publisher_id INT NOT NULL,
    publisher_name VARCHAR(200) NOT NULL,
    total_books INT NOT NULL DEFAULT 0,
    avg_rating DECIMAL(3,2) NULL,
    total_sold INT NULL,
    total_revenue DECIMAL(15,2) NULL,
    market_share DECIMAL(5,2) NULL,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (publisher_id),
    INDEX idx_publisher_name (publisher_name),
    INDEX idx_total_revenue (total_revenue),
    INDEX idx_market_share (market_share),
    INDEX idx_avg_rating (avg_rating),
    INDEX idx_last_updated (last_updated)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Publisher performance metrics for business intelligence';

-- =============================================
-- Create Views for Common Business Queries
-- =============================================

-- View: Top Authors by Revenue
CREATE OR REPLACE VIEW view_top_authors_revenue AS
SELECT 
    author_id,
    author_name,
    total_books,
    avg_rating,
    total_sold,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
FROM mart_author_insights
WHERE total_revenue IS NOT NULL
ORDER BY total_revenue DESC;

-- View: Category Performance Summary  
CREATE OR REPLACE VIEW view_category_summary AS
SELECT 
    category_path,
    total_books,
    avg_rating,
    avg_price,
    total_sold,
    total_revenue,
    market_share,
    CASE 
        WHEN market_share >= 10 THEN 'High'
        WHEN market_share >= 5 THEN 'Medium' 
        ELSE 'Low'
    END as market_position
FROM mart_category_performance
ORDER BY total_revenue DESC;

-- View: Daily Sales Trends (Last 30 days)
CREATE OR REPLACE VIEW view_daily_sales_trends AS
SELECT 
    date,
    COUNT(DISTINCT product_id) as products_sold,
    SUM(sold_today) as total_books_sold,
    SUM(sold_today * price) as total_revenue,
    AVG(rating) as avg_rating,
    AVG(price) as avg_price
FROM mart_daily_sales
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC;

-- View: Product Performance Dashboard
CREATE OR REPLACE VIEW view_product_dashboard AS
SELECT 
    product_id,
    product_name,
    category_path,
    publisher_name,
    total_sold,
    avg_rating,
    avg_price,
    CASE 
        WHEN total_sold >= 1000 THEN 'Bestseller'
        WHEN total_sold >= 500 THEN 'Popular'
        WHEN total_sold >= 100 THEN 'Moderate'
        ELSE 'Slow'
    END as sales_category,
    CASE 
        WHEN avg_rating >= 4.5 THEN 'Excellent'
        WHEN avg_rating >= 4.0 THEN 'Good'
        WHEN avg_rating >= 3.5 THEN 'Average'
        ELSE 'Below Average'
    END as rating_category
FROM mart_product_flat
WHERE total_sold > 0
ORDER BY total_sold DESC;

-- =============================================
-- Reset Foreign Key Checks
-- =============================================
SET FOREIGN_KEY_CHECKS = 1;

-- =============================================
-- Grant Permissions (Optional - adjust as needed)
-- =============================================
-- GRANT SELECT ON fahasa_datamart.* TO 'bi_user'@'%';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON fahasa_datamart.* TO 'etl_user'@'%';

-- =============================================
-- Table Statistics and Information
-- =============================================
-- After ETL population, run these for table statistics:
-- ANALYZE TABLE mart_author_insights;
-- ANALYZE TABLE mart_category_performance;
-- ANALYZE TABLE mart_daily_sales;
-- ANALYZE TABLE mart_product_flat;
-- ANALYZE TABLE mart_publisher_performance;

-- =============================================
-- End of Fahasa DataMart Schema
-- Total Tables: 5 mart tables
-- Total Views: 4 business intelligence views
-- Purpose: Ready for BI tools, reporting, and analytics
-- =============================================