-- =============================================================================
-- FAHASA DATA WAREHOUSE - MySQL Database Setup
-- =============================================================================
-- File: mysql_setup.sql
-- Purpose: Tạo 4 databases và các tables cơ bản cho MySQL
-- Author: Data Engineering Team
-- Created: 2024-01-27
-- =============================================================================

-- 1. TẠO CÁC DATABASES
-- =============================================================================

-- Drop existing databases (nếu cần reset)
-- DROP DATABASE IF EXISTS fahasa_staging;
-- DROP DATABASE IF EXISTS fahasa_control;
-- DROP DATABASE IF EXISTS fahasa_dw;
-- DROP DATABASE IF EXISTS fahasa_datamart;

-- Tạo databases mới
CREATE DATABASE IF NOT EXISTS fahasa_staging 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS fahasa_control 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS fahasa_dw 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS fahasa_datamart 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

-- =============================================================================
-- 2. STAGING DATABASE - Dữ liệu thô từ crawler
-- =============================================================================

USE fahasa_staging;

-- Bảng staging_books - Dữ liệu thô từ Fahasa crawler
CREATE TABLE IF NOT EXISTS staging_books (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Book Information
    title TEXT,
    author TEXT,
    publisher TEXT,
    supplier TEXT,
    
    -- Categories
    category_1 VARCHAR(255),
    category_2 VARCHAR(255),
    category_3 VARCHAR(500),
    
    -- Pricing
    original_price DECIMAL(15,2),
    discount_price DECIMAL(15,2),
    discount_percent DECIMAL(5,2),
    
    -- Ratings & Sales
    rating DECIMAL(3,2),
    rating_count INT,
    sold_count TEXT,
    sold_count_numeric INT,
    
    -- Book Details
    publish_year INT,
    language VARCHAR(100),
    page_count INT,
    weight DECIMAL(8,3),
    dimensions VARCHAR(100),
    
    -- URLs
    url TEXT,
    url_img TEXT,
    
    -- ETL Metadata
    time_collect TIMESTAMP,
    batch_id INT DEFAULT NULL,
    record_status VARCHAR(20) DEFAULT 'VALID',
    validation_errors JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_batch_id (batch_id),
    INDEX idx_record_status (record_status),
    INDEX idx_time_collect (time_collect),
    INDEX idx_supplier (supplier(100)),
    INDEX idx_category_1 (category_1),
    INDEX idx_url (url(255))
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 3. CONTROL DATABASE - ETL orchestration & monitoring
-- =============================================================================

USE fahasa_control;

-- Bảng staging_control_log - Theo dõi ETL batches
CREATE TABLE IF NOT EXISTS staging_control_log (
    batch_id INT AUTO_INCREMENT PRIMARY KEY,
    batch_date DATE DEFAULT (CURDATE()),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    status VARCHAR(20) DEFAULT 'RUNNING',
    
    -- Source Information
    source_type VARCHAR(100) NOT NULL,        -- 'CRAWLER', 'API', 'MANUAL'
    source_identifier VARCHAR(255),           -- script name, API endpoint
    
    -- Batch Metrics
    records_extracted INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    
    -- Performance
    duration_seconds INT,
    records_per_second DECIMAL(10,2),
    
    -- Error Handling
    error_message TEXT,
    error_count INT DEFAULT 0,
    
    -- Metadata
    created_by VARCHAR(100) DEFAULT 'system',
    notes TEXT,
    
    -- Indexes
    INDEX idx_batch_date (batch_date),
    INDEX idx_status (status),
    INDEX idx_source_type (source_type),
    INDEX idx_start_time (start_time)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bảng staging_validation_rules - Data quality rules
CREATE TABLE IF NOT EXISTS staging_validation_rules (
    rule_id INT AUTO_INCREMENT PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL UNIQUE,
    rule_description TEXT,
    rule_query TEXT NOT NULL,               -- SQL query trả về số lượng failed records
    
    -- Thresholds
    warning_threshold DECIMAL(5,2) DEFAULT 5.0,    -- % threshold for warning
    critical_threshold DECIMAL(5,2) DEFAULT 10.0,  -- % threshold for critical
    is_blocking BOOLEAN DEFAULT FALSE,              -- Stop ETL nếu fail
    
    -- Metadata
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    
    INDEX idx_active (is_active),
    INDEX idx_blocking (is_blocking)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bảng staging_data_quality - Kết quả data quality checks
CREATE TABLE IF NOT EXISTS staging_data_quality (
    quality_id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id INT NOT NULL,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Quality Check Info
    check_name VARCHAR(255) NOT NULL,
    check_description TEXT,
    check_query TEXT,
    
    -- Results
    total_records INT DEFAULT 0,
    passed_records INT DEFAULT 0,
    failed_records INT DEFAULT 0,
    status VARCHAR(20) NOT NULL,           -- 'PASS', 'WARNING', 'CRITICAL', 'FAILED'
    
    FOREIGN KEY (batch_id) REFERENCES staging_control_log(batch_id),
    INDEX idx_batch_id (batch_id),
    INDEX idx_status (status),
    INDEX idx_check_timestamp (check_timestamp)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert default validation rules
INSERT INTO staging_validation_rules 
(rule_name, rule_description, rule_query, warning_threshold, critical_threshold, is_blocking) 
VALUES
('missing_title', 'Kiểm tra books không có title', 
 'SELECT COUNT(*) FROM fahasa_staging.staging_books WHERE batch_id = ? AND (title IS NULL OR title = \'\')', 
 1.0, 5.0, TRUE),

('invalid_price', 'Kiểm tra giá âm hoặc 0', 
 'SELECT COUNT(*) FROM fahasa_staging.staging_books WHERE batch_id = ? AND (original_price <= 0 OR discount_price <= 0)', 
 5.0, 15.0, FALSE),

('missing_url', 'Kiểm tra books không có URL', 
 'SELECT COUNT(*) FROM fahasa_staging.staging_books WHERE batch_id = ? AND (url IS NULL OR url = \'\')', 
 2.0, 10.0, FALSE),

('invalid_rating', 'Kiểm tra rating ngoài khoảng 0-5', 
 'SELECT COUNT(*) FROM fahasa_staging.staging_books WHERE batch_id = ? AND (rating < 0 OR rating > 5)', 
 1.0, 5.0, FALSE),

('future_publish_year', 'Kiểm tra năm xuất bản trong tương lai', 
 'SELECT COUNT(*) FROM fahasa_staging.staging_books WHERE batch_id = ? AND publish_year > YEAR(CURDATE())', 
 0.1, 1.0, FALSE);

-- =============================================================================
-- 4. DATA WAREHOUSE DATABASE - Cleaned & transformed data
-- =============================================================================

USE fahasa_dw;

-- Dimension Tables
-- =============================================================================

-- Dim_Books - Book master data
CREATE TABLE IF NOT EXISTS dim_books (
    book_sk INT AUTO_INCREMENT PRIMARY KEY,
    book_nk VARCHAR(255) NOT NULL,         -- Natural key (URL hoặc book_id)
    
    -- Book Information
    title TEXT NOT NULL,
    author TEXT,
    publisher VARCHAR(255),
    supplier VARCHAR(255),
    
    -- Book Details
    publish_year INT,
    language VARCHAR(100),
    page_count INT,
    weight DECIMAL(8,3),
    dimensions VARCHAR(100),
    url TEXT,
    url_img TEXT,
    
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'FAHASA',
    
    UNIQUE KEY uk_book_effective (book_nk, effective_date),
    INDEX idx_book_nk (book_nk),
    INDEX idx_is_current (is_current),
    INDEX idx_effective_date (effective_date),
    INDEX idx_publisher (publisher),
    INDEX idx_supplier (supplier)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dim_Categories - Category hierarchy
CREATE TABLE IF NOT EXISTS dim_categories (
    category_sk INT AUTO_INCREMENT PRIMARY KEY,
    category_nk VARCHAR(500) NOT NULL,     -- Natural key (concatenated hierarchy)
    
    -- Category Hierarchy
    category_level_1 VARCHAR(255),
    category_level_2 VARCHAR(255), 
    category_level_3 VARCHAR(500),
    category_path VARCHAR(1000),           -- Full path separated by ' > '
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_category_nk (category_nk),
    INDEX idx_level_1 (category_level_1),
    INDEX idx_level_2 (category_level_2),
    INDEX idx_category_path (category_path(255))
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dim_Date - Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT PRIMARY KEY,                -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    
    -- Date Components
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20),
    week_of_year INT NOT NULL,
    
    -- Business attributes
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INT,
    fiscal_quarter INT,
    
    UNIQUE KEY uk_full_date (full_date),
    INDEX idx_year (year),
    INDEX idx_quarter (quarter),
    INDEX idx_month (month),
    INDEX idx_is_weekend (is_weekend)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fact Tables
-- =============================================================================

-- Fact_Book_Sales - Sales transactions (periodic snapshot)
CREATE TABLE IF NOT EXISTS fact_book_sales (
    sales_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Foreign Keys
    book_sk INT NOT NULL,
    category_sk INT NOT NULL, 
    date_sk INT NOT NULL,
    
    -- Measures
    original_price DECIMAL(15,2),
    discount_price DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    discount_percent DECIMAL(5,2),
    
    rating DECIMAL(3,2),
    rating_count INT DEFAULT 0,
    sold_count_numeric INT DEFAULT 0,
    
    -- Derived measures
    revenue_potential DECIMAL(18,2),        -- discount_price * sold_count_numeric
    discount_total DECIMAL(18,2),          -- discount_amount * sold_count_numeric
    
    -- Metadata
    batch_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (book_sk) REFERENCES dim_books(book_sk),
    FOREIGN KEY (category_sk) REFERENCES dim_categories(category_sk),
    FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    
    INDEX idx_book_sk (book_sk),
    INDEX idx_category_sk (category_sk),
    INDEX idx_date_sk (date_sk),
    INDEX idx_batch_id (batch_id),
    INDEX idx_created_at (created_at)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bridge Tables cho many-to-many relationships
-- =============================================================================

-- Bridge_Book_Categories - Many books can belong to multiple categories
CREATE TABLE IF NOT EXISTS bridge_book_categories (
    bridge_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    book_sk INT NOT NULL,
    category_sk INT NOT NULL,
    
    -- Weighting for allocation
    allocation_factor DECIMAL(5,4) DEFAULT 1.0000,
    
    -- Metadata
    effective_date DATE NOT NULL,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (book_sk) REFERENCES dim_books(book_sk),
    FOREIGN KEY (category_sk) REFERENCES dim_categories(category_sk),
    
    UNIQUE KEY uk_book_category_effective (book_sk, category_sk, effective_date),
    INDEX idx_book_sk (book_sk),
    INDEX idx_category_sk (category_sk),
    INDEX idx_is_current (is_current)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 5. DATA MART DATABASE - Aggregated data for reporting
-- =============================================================================

USE fahasa_datamart;

-- Aggregated Tables cho performance
-- =============================================================================

-- Agg_Daily_Sales - Daily aggregated sales by category
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    agg_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Dimensions
    date_sk INT NOT NULL,
    category_sk INT NOT NULL,
    supplier VARCHAR(255),
    
    -- Measures
    total_books INT DEFAULT 0,
    total_revenue DECIMAL(18,2) DEFAULT 0,
    total_discount DECIMAL(18,2) DEFAULT 0,
    avg_rating DECIMAL(3,2),
    total_sold_count BIGINT DEFAULT 0,
    
    -- Advanced metrics
    avg_discount_percent DECIMAL(5,2),
    price_range_min DECIMAL(15,2),
    price_range_max DECIMAL(15,2),
    
    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    batch_id INT,
    
    UNIQUE KEY uk_date_category_supplier (date_sk, category_sk, supplier),
    INDEX idx_date_sk (date_sk),
    INDEX idx_category_sk (category_sk),
    INDEX idx_supplier (supplier),
    INDEX idx_last_updated (last_updated)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Agg_Monthly_Categories - Monthly category performance
CREATE TABLE IF NOT EXISTS agg_monthly_categories (
    agg_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Time dimension
    year_month VARCHAR(7) NOT NULL,         -- Format: YYYY-MM
    year INT NOT NULL,
    month INT NOT NULL,
    
    -- Category
    category_sk INT NOT NULL,
    category_level_1 VARCHAR(255),
    category_level_2 VARCHAR(255),
    
    -- Measures
    total_books INT DEFAULT 0,
    total_revenue DECIMAL(18,2) DEFAULT 0,
    avg_price DECIMAL(15,2),
    avg_rating DECIMAL(3,2),
    total_ratings INT DEFAULT 0,
    
    -- Growth metrics
    revenue_growth_pct DECIMAL(8,2),       -- Month-over-month growth
    book_count_growth_pct DECIMAL(8,2),
    
    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_year_month_category (year_month, category_sk),
    INDEX idx_year_month (year_month),
    INDEX idx_category_sk (category_sk),
    INDEX idx_year (year),
    INDEX idx_month (month)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Summary Views cho reporting
-- =============================================================================

-- View: Top performing books
CREATE VIEW IF NOT EXISTS v_top_books AS
SELECT 
    db.book_sk,
    db.title,
    db.author,
    db.publisher,
    dc.category_path,
    fbs.rating,
    fbs.sold_count_numeric,
    fbs.discount_price,
    fbs.revenue_potential,
    dd.full_date
FROM fahasa_dw.fact_book_sales fbs
JOIN fahasa_dw.dim_books db ON fbs.book_sk = db.book_sk
JOIN fahasa_dw.dim_categories dc ON fbs.category_sk = dc.category_sk
JOIN fahasa_dw.dim_date dd ON fbs.date_sk = dd.date_sk
WHERE db.is_current = TRUE
AND fbs.rating >= 4.0
ORDER BY fbs.sold_count_numeric DESC, fbs.rating DESC;

-- =============================================================================
-- 6. INITIAL DATA POPULATION
-- =============================================================================

-- Populate date dimension với 5 năm data
USE fahasa_dw;

-- Insert sample date data (2022-2027)
INSERT INTO dim_date (date_sk, full_date, year, quarter, month, month_name, day, day_of_week, day_name, week_of_year, is_weekend)
WITH RECURSIVE date_series AS (
  SELECT 
    DATE('2022-01-01') as date_val
  UNION ALL
  SELECT 
    DATE_ADD(date_val, INTERVAL 1 DAY)
  FROM date_series 
  WHERE date_val < '2027-12-31'
)
SELECT 
    CAST(DATE_FORMAT(date_val, '%Y%m%d') AS UNSIGNED) as date_sk,
    date_val as full_date,
    YEAR(date_val) as year,
    QUARTER(date_val) as quarter,
    MONTH(date_val) as month,
    MONTHNAME(date_val) as month_name,
    DAY(date_val) as day,
    DAYOFWEEK(date_val) as day_of_week,
    DAYNAME(date_val) as day_name,
    WEEK(date_val) as week_of_year,
    CASE WHEN DAYOFWEEK(date_val) IN (1,7) THEN TRUE ELSE FALSE END as is_weekend
FROM date_series;

-- Insert default category
INSERT INTO dim_categories (category_nk, category_level_1, category_level_2, category_level_3, category_path)
VALUES 
('Sách trong nước > Văn học > Tiểu thuyết', 'Sách trong nước', 'Văn học', 'Tiểu thuyết', 'Sách trong nước > Văn học > Tiểu thuyết'),
('Sách trong nước > Kinh tế > Quản lý', 'Sách trong nước', 'Kinh tế', 'Quản lý', 'Sách trong nước > Kinh tế > Quản lý'),
('Sách trong nước > Khoa học > Công nghệ', 'Sách trong nước', 'Khoa học', 'Công nghệ', 'Sách trong nước > Khoa học > Công nghệ');

-- =============================================================================
-- 7. STORED PROCEDURES (MySQL)
-- =============================================================================

USE fahasa_control;

DELIMITER //

-- Procedure để check data quality
CREATE PROCEDURE sp_run_data_quality_check(
    IN p_batch_id INT
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_rule_name VARCHAR(255);
    DECLARE v_rule_query TEXT;
    DECLARE v_warn_threshold DECIMAL(5,2);
    DECLARE v_crit_threshold DECIMAL(5,2);
    DECLARE v_is_blocking BOOLEAN;
    DECLARE v_failed_count INT;
    DECLARE v_total_count INT;
    DECLARE v_failure_rate DECIMAL(5,2);
    DECLARE v_status VARCHAR(20);
    
    DECLARE rule_cursor CURSOR FOR 
        SELECT rule_name, rule_query, warning_threshold, critical_threshold, is_blocking
        FROM staging_validation_rules 
        WHERE is_active = TRUE
        ORDER BY is_blocking DESC;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN rule_cursor;
    
    rule_loop: LOOP
        FETCH rule_cursor INTO v_rule_name, v_rule_query, v_warn_threshold, v_crit_threshold, v_is_blocking;
        
        IF done THEN
            LEAVE rule_loop;
        END IF;
        
        -- Execute validation query
        SET @sql = v_rule_query;
        PREPARE stmt FROM @sql;
        EXECUTE stmt USING p_batch_id;
        -- Note: Would need dynamic SQL handling for results
        DEALLOCATE PREPARE stmt;
        
    END LOOP;
    
    CLOSE rule_cursor;
    
END //

DELIMITER ;

-- =============================================================================
-- 8. USERS & PERMISSIONS
-- =============================================================================

-- Tạo users cho different roles
-- CREATE USER 'etl_user'@'%' IDENTIFIED BY 'etl_password_2024';
-- CREATE USER 'analyst_user'@'%' IDENTIFIED BY 'analyst_password_2024';
-- CREATE USER 'app_user'@'%' IDENTIFIED BY 'app_password_2024';

-- Grant permissions
-- GRANT ALL PRIVILEGES ON fahasa_staging.* TO 'etl_user'@'%';
-- GRANT ALL PRIVILEGES ON fahasa_control.* TO 'etl_user'@'%';
-- GRANT ALL PRIVILEGES ON fahasa_dw.* TO 'etl_user'@'%';
-- GRANT SELECT, INSERT, UPDATE ON fahasa_datamart.* TO 'etl_user'@'%';

-- GRANT SELECT ON fahasa_dw.* TO 'analyst_user'@'%';
-- GRANT SELECT ON fahasa_datamart.* TO 'analyst_user'@'%';

-- GRANT SELECT ON fahasa_datamart.* TO 'app_user'@'%';

-- FLUSH PRIVILEGES;

-- =============================================================================
-- SETUP COMPLETED!
-- =============================================================================

SELECT 'MySQL setup completed successfully!' as status;
SELECT 'Databases created: fahasa_staging, fahasa_control, fahasa_dw, fahasa_datamart' as info;