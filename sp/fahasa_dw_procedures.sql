-- ================================================
-- FAHASA DATA WAREHOUSE - STORED PROCEDURES
-- Database: fahasa_dw
-- Total: 11 Stored Procedures
-- ================================================

USE fahasa_dw;

DELIMITER $$

-- ================================================
-- PROCEDURE: sp_load_author_dim
-- Purpose: Load authors (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_load_author_dim$$
CREATE PROCEDURE sp_load_author_dim()
BEGIN
    -- Insert new authors (FIXED: author instead of authors)
    INSERT INTO author_dim (author_name)
    SELECT DISTINCT 
        author
    FROM fahasa_staging.staging_books s
    WHERE author IS NOT NULL 
    AND author != ''
    AND NOT EXISTS (
        SELECT 1 FROM author_dim ad 
        WHERE ad.author_name = s.author
    );
    
    SELECT ROW_COUNT() as authors_inserted;
END$$

-- ================================================
-- PROCEDURE: sp_load_publisher_dim
-- Purpose: Load publishers with SCD Type 2
-- ================================================
DROP PROCEDURE IF EXISTS sp_load_publisher_dim$$
CREATE PROCEDURE sp_load_publisher_dim()
BEGIN
    DECLARE v_current_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    
    -- Insert new publishers
    INSERT INTO publisher_dim (publisher_name, effective_date, is_current)
    SELECT DISTINCT 
        publisher,
        v_current_time,
        1
    FROM fahasa_staging.staging_books s
    WHERE publisher IS NOT NULL 
    AND publisher != ''
    AND NOT EXISTS (
        SELECT 1 FROM publisher_dim pd 
        WHERE pd.publisher_name = s.publisher 
        AND pd.is_current = 1
    );
    
    SELECT ROW_COUNT() as publishers_inserted;
END$$

-- ================================================
-- PROCEDURE: sp_load_category_dim
-- Purpose: Load categories (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_load_category_dim$$
CREATE PROCEDURE sp_load_category_dim()
BEGIN
    -- Insert categories (FIXED: category_1 instead of category)
    INSERT IGNORE INTO category_dim (category_path, level_1, level_2, level_3)
    SELECT DISTINCT 
        CONCAT(COALESCE(category_1,''), COALESCE(CONCAT('/',category_2),''), COALESCE(CONCAT('/',category_3),'')),
        category_1,
        category_2,
        category_3
    FROM fahasa_staging.staging_books s
    WHERE category_1 IS NOT NULL AND category_1 != '';
    
    SELECT ROW_COUNT() as categories_processed;
END$$

-- ================================================
-- PROCEDURE: sp_load_product_dim
-- Purpose: Load products with SCD Type 2
-- ================================================
-- ================================================
-- PROCEDURE: sp_load_product_dim
-- Purpose: Load products (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_load_product_dim$$
CREATE PROCEDURE sp_load_product_dim()
BEGIN
    -- Insert new products (FIXED: removed non-existent columns)
    INSERT IGNORE INTO product_dim (
        product_name, language, page_count, weight, 
        dimensions, url_path
    )
    SELECT DISTINCT
        s.title,
        s.language,
        s.page_count,
        s.weight,
        s.dimensions,
        s.url
    FROM fahasa_staging.staging_books s
    WHERE s.title IS NOT NULL AND s.title != '';
    
    SELECT ROW_COUNT() as products_inserted;
END$$

-- ================================================
-- PROCEDURE: sp_simple_load_publishers
-- Purpose: Simple publisher loading
-- ================================================
DROP PROCEDURE IF EXISTS sp_simple_load_publishers$$
CREATE PROCEDURE sp_simple_load_publishers()
BEGIN
    INSERT IGNORE INTO publisher_dim (publisher_name)
    SELECT DISTINCT publisher
    FROM fahasa_staging.staging_books
    WHERE publisher IS NOT NULL AND publisher != '';
    
    SELECT ROW_COUNT() as publishers_loaded;
END$$

-- ================================================
-- PROCEDURE: sp_simple_load_product_author_bridge
-- Purpose: Load product-author relationships (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_simple_load_product_author_bridge$$
CREATE PROCEDURE sp_simple_load_product_author_bridge()
BEGIN
    INSERT IGNORE INTO product_author_bridge (product_id, author_id)
    SELECT DISTINCT 
        p.id as product_id,
        a.author_id
    FROM fahasa_staging.staging_books s
    JOIN product_dim p ON p.product_name = s.title
    JOIN author_dim a ON a.author_name = s.author
    WHERE s.author IS NOT NULL AND s.author != '';
    
    SELECT ROW_COUNT() as author_links_created;
END$$
    
    SELECT ROW_COUNT() as author_links_created;
END$$

-- ================================================
-- PROCEDURE: sp_simple_load_product_category_bridge
-- Purpose: Load product-category relationships (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_simple_load_product_category_bridge$$
CREATE PROCEDURE sp_simple_load_product_category_bridge()
BEGIN
    INSERT IGNORE INTO product_category_bridge (product_id, category_id)
    SELECT DISTINCT 
        p.id as product_id,
        c.category_id
    FROM fahasa_staging.staging_books s
    JOIN product_dim p ON p.product_name = s.title
    JOIN category_dim c ON c.level_1 = s.category_1
    WHERE s.category_1 IS NOT NULL AND s.category_1 != '';
    
    SELECT ROW_COUNT() as category_links_created;
END$$
    
    SELECT ROW_COUNT() as category_links_created;
END$$

-- ================================================
-- PROCEDURE: sp_simple_load_product_publisher_bridge
-- Purpose: Load product-publisher relationships (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_simple_load_product_publisher_bridge$$
CREATE PROCEDURE sp_simple_load_product_publisher_bridge()
BEGIN
    INSERT IGNORE INTO product_publisher_bridge (product_id, publisher_id)
    SELECT DISTINCT 
        p.id as product_id,
        pub.publisher_id
    FROM fahasa_staging.staging_books s
    JOIN product_dim p ON p.product_name = s.title
    JOIN publisher_dim pub ON pub.publisher_name = s.publisher
    WHERE s.publisher IS NOT NULL AND s.publisher != '';
    
    SELECT ROW_COUNT() as publisher_links_created;
END$$
    
    SELECT ROW_COUNT() as publisher_links_created;
END$$

-- ================================================
-- PROCEDURE: sp_complete_load_fact_metrics
-- Purpose: Load fact metrics table (FIXED)
-- ================================================
DROP PROCEDURE IF EXISTS sp_complete_load_fact_metrics$$
CREATE PROCEDURE sp_complete_load_fact_metrics()
BEGIN
    DECLARE v_current_date_sk INT;
    DECLARE v_batch_id INT DEFAULT 1;
    
    -- Get current date_sk (FIXED: using full_date instead of date_value)
    SELECT COALESCE(MAX(date_sk), 20251121) INTO v_current_date_sk 
    FROM fahasa_dw.date_dim 
    WHERE full_date = CURDATE();
    
    -- If no date found, insert today's date
    IF v_current_date_sk = 20251121 THEN
        INSERT IGNORE INTO date_dim (date_sk, full_date, day_since_2005) 
        VALUES (20251121, CURDATE(), DATEDIFF(CURDATE(), '2005-01-01'));
        SET v_current_date_sk = 20251121;
    END IF;
    
    INSERT IGNORE INTO fact_book_sales (
        book_sk, category_sk, date_sk, original_price, discount_price,
        discount_amount, discount_percent, rating, rating_count,
        sold_count_numeric, revenue_potential, discount_total, batch_id
    )
    SELECT DISTINCT
        p.id as book_sk,
        COALESCE(c.category_id, 1) as category_sk,
        v_current_date_sk as date_sk,
        COALESCE(s.original_price, 0) as original_price,
        COALESCE(s.discount_price, s.original_price, 0) as discount_price,
        COALESCE(s.original_price - s.discount_price, 0) as discount_amount,
        COALESCE(CAST(REPLACE(s.discount_percent, '%', '') AS DECIMAL(5,2)), 0) as discount_percent,
        COALESCE(s.rating, 0) as rating,
        COALESCE(s.rating_count, 0) as rating_count,
        COALESCE(s.sold_count_numeric, 0) as sold_count_numeric,
        COALESCE(s.discount_price * s.sold_count_numeric, 0) as revenue_potential,
        COALESCE((s.original_price - s.discount_price) * s.sold_count_numeric, 0) as discount_total,
        v_batch_id as batch_id
    FROM fahasa_staging.staging_books s
    JOIN product_dim p ON p.product_name = s.title
    LEFT JOIN product_category_bridge pcb ON p.id = pcb.product_id
    LEFT JOIN category_dim c ON pcb.category_id = c.category_id
    WHERE s.title IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM fact_book_sales f 
        WHERE f.book_sk = p.id AND f.date_sk = v_current_date_sk
    );
    
    SELECT ROW_COUNT() as fact_records_inserted;
END$$

-- ================================================
-- PROCEDURE: sp_complete_master_etl
-- Purpose: Complete ETL orchestration
-- ================================================
DROP PROCEDURE IF EXISTS sp_complete_master_etl$$
CREATE PROCEDURE sp_complete_master_etl()
BEGIN
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    DECLARE v_total_processed INT DEFAULT 0;
    
    -- Load dimensions
    CALL sp_load_author_dim();
    CALL sp_load_publisher_dim();
    CALL sp_load_category_dim();
    CALL sp_load_product_dim();
    
    -- Load bridges
    CALL sp_simple_load_product_author_bridge();
    CALL sp_simple_load_product_category_bridge();
    CALL sp_simple_load_product_publisher_bridge();
    
    -- Load facts
    CALL sp_complete_load_fact_metrics();
    
    -- Summary
    SELECT 
        'Complete Master ETL finished' as status,
        TIMESTAMPDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP) as duration_seconds,
        (SELECT COUNT(*) FROM product_dim WHERE is_current = 1) as products,
        (SELECT COUNT(*) FROM author_dim WHERE is_current = 1) as authors,
        (SELECT COUNT(*) FROM fact_daily_product_metrics WHERE fact_date = CURDATE()) as facts;
END$$

-- ================================================
-- PROCEDURE: sp_ultimate_master_etl
-- Purpose: Complete ETL with aggregates
-- ================================================
DROP PROCEDURE IF EXISTS sp_ultimate_master_etl$$
CREATE PROCEDURE sp_ultimate_master_etl()
BEGIN
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    
    -- Core ETL
    CALL sp_complete_master_etl();
    
    -- Business Intelligence Aggregates
    CALL sp_populate_author_performance();
    CALL sp_populate_publisher_revenue();
    CALL sp_populate_category_sales();
    CALL sp_populate_price_range();
    CALL sp_populate_rating_distribution();
    
    -- Final summary
    SELECT 
        '🎯 ULTIMATE ETL COMPLETED!' as status,
        TIMESTAMPDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP) as total_duration_seconds,
        CURDATE() as processed_date,
        (SELECT COUNT(*) FROM author_performance_aggregate) as author_kpis,
        (SELECT COUNT(*) FROM publisher_revenue_aggregate) as publisher_analytics,
        (SELECT COUNT(*) FROM category_sales_aggregate) as category_metrics;
END$$

DELIMITER ;

-- ================================================
-- VERIFICATION
-- ================================================
SELECT 'FAHASA DW PROCEDURES DEPLOYED SUCCESSFULLY!' as status;
SHOW PROCEDURE STATUS WHERE Db = 'fahasa_dw';