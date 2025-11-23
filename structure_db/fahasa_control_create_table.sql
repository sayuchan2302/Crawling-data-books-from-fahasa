USE fahasa_control;

DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS config;

CREATE TABLE config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `file_name` VARCHAR(255),
    `file_path` VARCHAR(255),
    `file_encoding` VARCHAR(255),
    `crawl_frequency` INT,
    `data_size` INT,
    `retry_count` INT,
    `timeout` INT,
    `dw_source_port` INT,
    `staging_source_port` INT,
    `source_path` VARCHAR(255),
    `destination_path` VARCHAR(255),
    `backup_path` VARCHAR(255),
    `file_type` ENUM('csv','json','xml'),
    `delimiter` VARCHAR(255),

    `dw_source_host` VARCHAR(255),
    `dw_source_password` VARCHAR(255),
    `dw_source_username` VARCHAR(255),

    `staging_source_host` VARCHAR(255),
    `staging_source_password` VARCHAR(255),
    `staging_source_username` VARCHAR(255),

    `columns` LONGTEXT,
    `tables` VARCHAR(255),
    `note` VARCHAR(255),
    `notification_emails` VARCHAR(255),

    `create_time` DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    `update_time` DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    `created_by` VARCHAR(255),
    `updated_by` VARCHAR(255),
    `version` VARCHAR(255),
    `is_active` BIT DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_config INT,
    `count` INT,
    `log_level` ENUM('INFO','WARN','ERROR'),
    `status` ENUM('SUCCESS','FAILED','RUNNING'),
    `create_time` DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
    `update_time` DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    `created_by` VARCHAR(255),
    `destination_path` VARCHAR(255),
    `error_message` TEXT,
    `location` VARCHAR(255),
    `stack_trace` LONGTEXT,

    INDEX idx_id_config (id_config),
    INDEX idx_log_level (log_level),
    INDEX idx_status (status),
    INDEX idx_create_time (create_time),

    CONSTRAINT fk_logs_config
        FOREIGN KEY (id_config) REFERENCES config(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
