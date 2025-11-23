-- ============================================================
-- Stored Procedures for Database: fahasa_control
-- Generated on: 2025-11-23 16:35:35
-- Total procedures: 3
-- ============================================================

USE `fahasa_control`;

-- ==================================================
-- Procedure: sp_end_operation_log
-- Type: PROCEDURE
-- Created: root@localhost
-- Modified: 2025-11-23 13:23:28
-- ==================================================

DROP PROCEDURE IF EXISTS `sp_end_operation_log`;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_end_operation_log`(
                IN p_log_id INT,
                IN p_status VARCHAR(20),
                IN p_records_processed INT,
                IN p_records_inserted INT,
                IN p_records_updated INT,
                IN p_records_failed INT,
                IN p_error_message TEXT
            )
BEGIN
                UPDATE etl_logs 
                SET 
                    end_time = NOW(),
                    status = p_status,
                    records_processed = p_records_processed,
                    records_inserted = p_records_inserted,
                    records_updated = p_records_updated,
                    records_failed = p_records_failed,
                    error_message = p_error_message,
                    updated_at = NOW()
                WHERE log_id = p_log_id;
                
                SELECT 
                    log_id,
                    operation_name,
                    status,
                    TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds
                FROM etl_logs 
                WHERE log_id = p_log_id;
            END
$$

DELIMITER ;

-- --------------------------------------------------

-- ==================================================
-- Procedure: sp_log_data_quality
-- Type: PROCEDURE
-- Created: root@localhost
-- Modified: 2025-11-23 13:23:28
-- ==================================================

DROP PROCEDURE IF EXISTS `sp_log_data_quality`;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_log_data_quality`(
                IN p_table_name VARCHAR(100),
                IN p_check_type VARCHAR(50),
                IN p_expected_value VARCHAR(255),
                IN p_actual_value VARCHAR(255),
                IN p_check_status VARCHAR(20),
                IN p_details TEXT
            )
BEGIN
                INSERT INTO data_quality_logs (
                    table_name,
                    quality_check_type,
                    expected_value,
                    actual_value,
                    check_status,
                    check_details,
                    checked_at
                ) VALUES (
                    p_table_name,
                    p_check_type,
                    p_expected_value,
                    p_actual_value,
                    p_check_status,
                    p_details,
                    NOW()
                );
                
                SELECT LAST_INSERT_ID() as quality_log_id, 'Quality check logged' as message;
            END
$$

DELIMITER ;

-- --------------------------------------------------

-- ==================================================
-- Procedure: sp_start_operation_log
-- Type: PROCEDURE
-- Created: root@localhost
-- Modified: 2025-11-23 13:23:28
-- ==================================================

DROP PROCEDURE IF EXISTS `sp_start_operation_log`;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_start_operation_log`(
                IN p_operation_name VARCHAR(100),
                IN p_operation_type VARCHAR(20),
                IN p_details TEXT,
                OUT p_log_id INT
            )
BEGIN
                INSERT INTO etl_logs (
                    operation_name, 
                    operation_type, 
                    status, 
                    execution_details,
                    start_time
                ) VALUES (
                    p_operation_name, 
                    p_operation_type, 
                    'STARTED', 
                    p_details,
                    NOW()
                );
                
                SET p_log_id = LAST_INSERT_ID();
                
                SELECT p_log_id as log_id, 'Operation started successfully' as message;
            END
$$

DELIMITER ;

-- --------------------------------------------------

