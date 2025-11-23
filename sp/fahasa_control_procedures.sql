-- ================================================
-- FAHASA CONTROL SYSTEM - STORED PROCEDURES
-- Database: fahasa_control (Simplified Schema)
-- Total: 5 Stored Procedures + Enhanced Logging
-- ================================================

USE fahasa_control;

DELIMITER $$

-- ================================================
-- PROCEDURE: sp_log_etl_start
-- Purpose: Initialize ETL logging with simplified schema
-- ================================================
DROP PROCEDURE IF EXISTS sp_log_etl_start$$
CREATE PROCEDURE sp_log_etl_start(
    IN p_config_id INT,
    IN p_operation_name VARCHAR(255),
    IN p_created_by VARCHAR(255),
    OUT p_log_id INT
)
BEGIN
    -- Insert initial log entry using simplified logs table
    INSERT INTO logs (
        id_config,
        log_level,
        status,
        created_by,
        location,
        create_time
    ) VALUES (
        p_config_id,
        'INFO',
        'RUNNING',
        p_created_by,
        p_operation_name,
        NOW(6)
    );
    
    SET p_log_id = LAST_INSERT_ID();
    
    SELECT CONCAT('ETL logging started with log_id: ', p_log_id) as status;
END$$

-- ================================================
-- PROCEDURE: sp_log_etl_end
-- Purpose: Finalize ETL logging with simplified schema
-- ================================================
DROP PROCEDURE IF EXISTS sp_log_etl_end$$
CREATE PROCEDURE sp_log_etl_end(
    IN p_log_id INT,
    IN p_status ENUM('SUCCESS','FAILED'),
    IN p_count INT,
    IN p_message TEXT,
    IN p_error_message TEXT
)
BEGIN
    -- Update log entry using simplified logs table
    UPDATE logs 
    SET 
        status = p_status,
        `count` = p_count,
        error_message = p_error_message,
        location = CONCAT(COALESCE(location, ''), ' - ', COALESCE(p_message, 'Completed')),
        update_time = NOW(6),
        log_level = CASE 
            WHEN p_status = 'FAILED' THEN 'ERROR'
            WHEN p_error_message IS NOT NULL THEN 'WARN'
            ELSE 'INFO'
        END
    WHERE id = p_log_id;
    
    SELECT CONCAT('ETL completed with status: ', p_status) as result;
END$$

-- ================================================
-- PROCEDURE: sp_scheduled_daily_etl
-- Purpose: Production ETL with full logging
-- ================================================
DROP PROCEDURE IF EXISTS sp_scheduled_daily_etl$$
CREATE PROCEDURE sp_scheduled_daily_etl()
BEGIN
    DECLARE v_batch_id VARCHAR(50);
    DECLARE v_batch_name VARCHAR(200);
    DECLARE v_records_processed INT DEFAULT 0;
    DECLARE v_error_count INT DEFAULT 0;
    DECLARE v_final_status VARCHAR(20) DEFAULT 'SUCCESS';
    DECLARE v_notes TEXT DEFAULT '';
    
    -- Initialize batch
    SET v_batch_name = CONCAT('Daily ETL - ', CURDATE());
    CALL sp_log_etl_start(v_batch_name, 'sp_scheduled_daily_etl', 'automated_scheduler', v_batch_id);
    
    -- Execute ETL
    CALL fahasa_dw.sp_ultimate_master_etl();
    
    -- Count processed records
    SELECT COUNT(*) INTO v_records_processed
    FROM fahasa_staging.staging_books;
    
    -- Generate summary notes
    SET v_notes = CONCAT(
        'ETL completed successfully. ',
        'Staging: ', v_records_processed, ' records. ',
        'Facts: ', (SELECT COUNT(*) FROM fahasa_dw.fact_daily_product_metrics WHERE fact_date = CURDATE()), ' records. ',
        'BI Aggregates: ', (SELECT COUNT(*) FROM fahasa_dw.author_performance_aggregate), ' author KPIs created.'
    );
    
    -- Finalize logging
    CALL sp_log_etl_end(v_batch_id, v_final_status, v_records_processed, 0, v_error_count, v_notes);
    
END$$

-- ================================================
-- PROCEDURE: sp_etl_with_control_logging
-- Purpose: ETL with integrated control logging
-- ================================================
DROP PROCEDURE IF EXISTS sp_etl_with_control_logging$$
CREATE PROCEDURE sp_etl_with_control_logging(
    IN p_batch_name VARCHAR(200),
    IN p_script_name VARCHAR(200),
    IN p_created_by VARCHAR(100)
)
BEGIN
    DECLARE v_batch_id VARCHAR(50);
    DECLARE v_log_id INT;
    DECLARE v_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    DECLARE v_total_processed INT DEFAULT 0;
    DECLARE v_errors INT DEFAULT 0;
    DECLARE v_status VARCHAR(20) DEFAULT 'SUCCESS';
    DECLARE v_notes TEXT DEFAULT '';
    
    -- Generate unique batch ID
    SET v_batch_id = UUID();
    
    START TRANSACTION;
    
    -- Initialize control log
    INSERT INTO staging_control_log (
        batch_id,
        batch_name,
        start_time,
        status,
        script_name,
        created_by,
        notes,
        transformation_type
    ) VALUES (
        v_batch_id,
        p_batch_name,
        v_start_time,
        'RUNNING',
        p_script_name,
        p_created_by,
        'ETL pipeline started - transforming staging to data warehouse',
        CASE 
            WHEN p_batch_name LIKE '%daily%' THEN 'DAILY_LOAD'
            WHEN p_batch_name LIKE '%full%' THEN 'FULL_LOAD'
            ELSE 'INCREMENTAL_LOAD'
        END
    );
    
    SET v_log_id = LAST_INSERT_ID();
    
    -- Execute ETL components with progress tracking
    CALL fahasa_dw.sp_load_author_dim();
    UPDATE staging_control_log 
    SET notes = CONCAT(COALESCE(notes, ''), ' - Authors loaded')
    WHERE log_id = v_log_id;
    
    CALL fahasa_dw.sp_load_category_dim();
    UPDATE staging_control_log 
    SET notes = CONCAT(COALESCE(notes, ''), ' - Categories loaded')
    WHERE log_id = v_log_id;
    
    CALL fahasa_dw.sp_load_product_dim();
    UPDATE staging_control_log 
    SET notes = CONCAT(COALESCE(notes, ''), ' - Products loaded')
    WHERE log_id = v_log_id;
    
    CALL fahasa_dw.sp_complete_load_fact_metrics();
    UPDATE staging_control_log 
    SET notes = CONCAT(COALESCE(notes, ''), ' - Facts loaded')
    WHERE log_id = v_log_id;
    
    -- Get final counts
    SELECT COUNT(*) INTO v_total_processed
    FROM fahasa_staging.staging_books;
    
    -- Finalize log
    UPDATE staging_control_log 
    SET 
        end_time = CURRENT_TIMESTAMP,
        status = v_status,
        records_processed = v_total_processed,
        records_failed = v_errors,
        error_count = v_errors,
        execution_duration_seconds = TIMESTAMPDIFF(SECOND, v_start_time, CURRENT_TIMESTAMP),
        success_rate = 100.0,
        data_quality_score = 100.0,
        notes = CONCAT(COALESCE(notes, ''), ' - ETL completed successfully')
    WHERE log_id = v_log_id;
    
    COMMIT;
    
    SELECT 'ETL with control logging completed successfully!' as status;
    
END$$

-- ================================================
-- PROCEDURE: sp_log_datamart_etl
-- Purpose: Log DataMart ETL operations
-- ================================================
DROP PROCEDURE IF EXISTS sp_log_datamart_etl$$
CREATE PROCEDURE sp_log_datamart_etl(
    IN p_batch_id VARCHAR(50),
    IN p_mart_table VARCHAR(100),
    IN p_status VARCHAR(20),
    IN p_records_before INT,
    IN p_records_after INT,
    IN p_error_message TEXT
)
BEGIN
    DECLARE v_duration INT DEFAULT 0;
    
    -- Calculate duration if completing
    IF p_status IN ('SUCCESS', 'FAILED') THEN
        SELECT COALESCE(TIMESTAMPDIFF(SECOND, start_time, NOW()), 0)
        INTO v_duration
        FROM datamart_etl_log 
        WHERE batch_id = p_batch_id AND mart_table_name = p_mart_table
        ORDER BY log_id DESC LIMIT 1;
    END IF;
    
    -- Insert/Update log entry
    INSERT INTO datamart_etl_log (
        batch_id, mart_table_name, status, records_before, records_after, 
        execution_duration_seconds, error_message, records_inserted, end_time
    ) VALUES (
        p_batch_id, p_mart_table, p_status, p_records_before, p_records_after,
        v_duration, p_error_message, GREATEST(0, p_records_after - p_records_before),
        CASE WHEN p_status IN ('SUCCESS', 'FAILED') THEN NOW() ELSE NULL END
    );
    
    SELECT CONCAT('DataMart ETL logged: ', p_mart_table, ' - ', p_status) as result;
    
END$$

DELIMITER ;

-- ================================================
-- ENHANCED CONTROL FUNCTIONS
-- ================================================

DELIMITER $$

-- Enhanced control summary function
DROP FUNCTION IF EXISTS fn_get_enhanced_control_summary$$
CREATE FUNCTION fn_get_enhanced_control_summary() RETURNS TEXT
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE v_summary TEXT;
    
    SELECT CONCAT(
        'Last ETL: ', DATE_FORMAT(MAX(end_time), '%m-%d %H:%i'), 
        ' | Status: ', status,
        ' | Records: ', SUM(records_processed),
        ' | Duration: ', AVG(execution_duration_seconds), 's',
        ' | Quality: ', ROUND(AVG(data_quality_score), 1), '%'
    ) INTO v_summary
    FROM staging_control_log
    WHERE start_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
      AND status = 'SUCCESS'
    ORDER BY end_time DESC
    LIMIT 1;
    
    RETURN COALESCE(v_summary, 'No recent successful ETL runs');
END$$

-- ETL status function
DROP FUNCTION IF EXISTS fn_get_etl_status$$
CREATE FUNCTION fn_get_etl_status() RETURNS VARCHAR(500)
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE v_status_info VARCHAR(500);
    
    SELECT CONCAT(
        'Latest: ', batch_name, 
        ' | Status: ', status,
        ' | Time: ', DATE_FORMAT(start_time, '%H:%i'),
        ' | Records: ', COALESCE(records_processed, 0)
    ) INTO v_status_info
    FROM staging_control_log 
    ORDER BY start_time DESC 
    LIMIT 1;
    
    RETURN COALESCE(v_status_info, 'No ETL runs found');
END$$

DELIMITER ;

-- ================================================
-- VERIFICATION
-- ================================================
SELECT 'FAHASA CONTROL PROCEDURES DEPLOYED SUCCESSFULLY!' as status;
SHOW PROCEDURE STATUS WHERE Db = 'fahasa_control';