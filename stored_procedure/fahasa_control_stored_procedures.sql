-- ============================================================
-- Stored Procedures for Database: fahasa_control
-- Generated on: 2025-11-25 01:31:15
-- Total procedures: 3
-- ============================================================

USE `fahasa_control`;

-- ==================================================
-- Procedure: sp_end_operation_log
-- Type: PROCEDURE
-- Created: root@localhost
-- Modified: 2025-11-24 02:26:25
-- ==================================================

DROP PROCEDURE IF EXISTS `sp_end_operation_log`;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_end_operation_log`(
    IN p_log_id INT,
    IN p_status VARCHAR(50),
    IN p_records_processed INT
)
BEGIN
    UPDATE logs 
    SET 
        status = p_status,
        count = p_records_processed,
        update_time = NOW(),
        log_level = CASE 
            WHEN p_status = 'SUCCESS' THEN 'INFO'
            WHEN p_status = 'FAILED' THEN 'ERROR'
            ELSE 'WARN'
        END
    WHERE id = p_log_id;
END
$$

DELIMITER ;

-- --------------------------------------------------

-- ==================================================
-- Procedure: sp_log_data_quality
-- Type: PROCEDURE
-- Created: root@localhost
-- Modified: 2025-11-24 02:26:25
-- ==================================================

DROP PROCEDURE IF EXISTS `sp_log_data_quality`;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_log_data_quality`(
    IN p_table_name VARCHAR(255),
    IN p_actual_value VARCHAR(255),
    IN p_status VARCHAR(20)
)
BEGIN
    DECLARE config_id INT DEFAULT 1;
    
    INSERT INTO logs (
        id_config, count, log_level, status,
        create_time, update_time, created_by,
        destination_path, location
    ) VALUES (
        config_id, 
        CAST(p_actual_value AS UNSIGNED),
        IF(p_status = 'PASSED', 'INFO', 'WARN'),
        IF(p_status = 'PASSED', 'SUCCESS', 'FAILED'),
        NOW(), NOW(),
        CONCAT('quality_check_', p_table_name),
        'QUALITY_CHECK',
        p_table_name
    );
END
$$

DELIMITER ;

-- --------------------------------------------------

-- ==================================================
-- Procedure: sp_start_operation_log
-- Type: PROCEDURE
-- Created: root@localhost
-- Modified: 2025-11-24 02:26:25
-- ==================================================

DROP PROCEDURE IF EXISTS `sp_start_operation_log`;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_start_operation_log`(
    IN p_operation_name VARCHAR(255),
    IN p_operation_type VARCHAR(50),
    IN p_description TEXT
)
BEGIN
    DECLARE config_id INT DEFAULT 1;
    
    INSERT INTO logs (
        id_config, log_level, status,
        create_time, update_time, created_by,
        destination_path, error_message, location
    ) VALUES (
        config_id, 'INFO', 'RUNNING',
        NOW(), NOW(), p_operation_name,
        p_operation_type, p_description, 'ETL_PIPELINE'
    );
    
    SELECT LAST_INSERT_ID() as log_id, 'RUNNING' as status;
END
$$

DELIMITER ;

-- --------------------------------------------------

