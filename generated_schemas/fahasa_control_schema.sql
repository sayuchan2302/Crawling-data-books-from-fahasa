-- =============================================
-- Fahasa Control Database Schema (Enhanced Version)
-- Generated on: 2025-11-23 13:00:01
-- Purpose: Enhanced ETL monitoring, logging, and quality control
-- Enhanced with: 77 additional columns across 5 tables (156 total)
-- =============================================

-- Create database
CREATE DATABASE IF NOT EXISTS fahasa_control;
USE fahasa_control;

-- Set charset for proper text handling
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- =============================================
-- Table: config (57 columns)
-- Purpose: Configuration settings for crawlers, ETL processes, and system parameters
-- =============================================
DROP TABLE IF EXISTS `config`;

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
  `config_name` varchar(100) DEFAULT NULL,
  `config_type` enum('CRAWLER','ETL','GENERAL') DEFAULT NULL,
  `source_url` varchar(500) DEFAULT NULL,
  `target_books` int DEFAULT NULL,
  `max_retry` int DEFAULT NULL,
  `timeout_seconds` int DEFAULT NULL,
  `user_agent` varchar(255) DEFAULT NULL,
  `delay_min` int DEFAULT NULL,
  `delay_max` int DEFAULT NULL,
  `staging_host` varchar(255) DEFAULT NULL,
  `staging_port` int DEFAULT NULL,
  `staging_username` varchar(255) DEFAULT NULL,
  `staging_password` varchar(255) DEFAULT NULL,
  `staging_database` varchar(255) DEFAULT NULL,
  `dw_host` varchar(255) DEFAULT NULL,
  `dw_port` int DEFAULT NULL,
  `dw_username` varchar(255) DEFAULT NULL,
  `dw_password` varchar(255) DEFAULT NULL,
  `dw_database` varchar(255) DEFAULT NULL,
  `etl_batch_size` int DEFAULT NULL,
  `parallel_processes` int DEFAULT NULL,
  `error_threshold` decimal(5,2) DEFAULT NULL,
  `data_retention_days` int DEFAULT NULL,
  `alert_on_failure` tinyint(1) DEFAULT NULL,
  `performance_threshold` int DEFAULT NULL,
  `description` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Column Details for config (57 columns):
-- id                        int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- file_name                 varchar(255)        
-- file_path                 varchar(255)        
-- file_encoding             varchar(255)        
-- crawl_frequency           int                 
-- data_size                 int                 
-- retry_count               int                 
-- timeout                   int                 
-- dw_source_port            int                 
-- staging_source_port       int                 
-- source_path               varchar(255)        
-- destination_path          varchar(255)        
-- backup_path               varchar(255)        
-- file_type                 enum('csv','json','xml')
-- delimiter                 varchar(255)        
-- dw_source_host            varchar(255)        
-- dw_source_password        varchar(255)        
-- dw_source_username        varchar(255)        
-- staging_source_host       varchar(255)        
-- staging_source_password   varchar(255)        
-- staging_source_username   varchar(255)        
-- columns                   longtext            
-- tables                    varchar(255)        
-- note                      varchar(255)        
-- notification_emails       varchar(255)        
-- create_time               datetime(6)          -- DEFAULT 'CURRENT_TIMESTAMP(6)', DEFAULT_GENERATED
-- update_time               datetime(6)          -- DEFAULT 'CURRENT_TIMESTAMP(6)', DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP(6)
-- created_by                varchar(255)        
-- updated_by                varchar(255)        
-- version                   varchar(255)        
-- is_active                 bit(1)               -- DEFAULT 'b'1''
-- config_name               varchar(100)        
-- config_type               enum('CRAWLER','ETL','GENERAL')
-- source_url                varchar(500)        
-- target_books              int                 
-- max_retry                 int                 
-- timeout_seconds           int                 
-- user_agent                varchar(255)        
-- delay_min                 int                 
-- delay_max                 int                 
-- staging_host              varchar(255)        
-- staging_port              int                 
-- staging_username          varchar(255)        
-- staging_password          varchar(255)        
-- staging_database          varchar(255)        
-- dw_host                   varchar(255)        
-- dw_port                   int                 
-- dw_username               varchar(255)        
-- dw_password               varchar(255)        
-- dw_database               varchar(255)        
-- etl_batch_size            int                 
-- parallel_processes        int                 
-- error_threshold           decimal(5,2)        
-- data_retention_days       int                 
-- alert_on_failure          tinyint(1)          
-- performance_threshold     int                 
-- description               text                

-- Sample configuration insert:
-- INSERT INTO config (file_name, file_path, source_url, target_books, batch_size, crawl_frequency, user_agent) 
-- VALUES ('fahasa_crawler', '/path/to/crawler.py', 'https://fahasa.com', 100, 50, 'daily', 'Fahasa_Crawler/1.0');

-- End of config table

-- =============================================
-- Table: logs (21 columns)
-- Purpose: Legacy logging table with enhanced session correlation
-- =============================================
DROP TABLE IF EXISTS `logs`;

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
  `session_id` varchar(36) DEFAULT NULL,
  `operation_name` varchar(100) DEFAULT NULL,
  `execution_time_ms` bigint DEFAULT NULL,
  `memory_usage` decimal(10,2) DEFAULT NULL,
  `source_path` varchar(255) DEFAULT NULL,
  `file_size_bytes` bigint DEFAULT NULL,
  `error_code` varchar(20) DEFAULT NULL,
  `additional_info` longtext,
  `tags` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_id_config` (`id_config`),
  KEY `idx_log_level` (`log_level`),
  KEY `idx_status` (`status`),
  KEY `idx_create_time` (`create_time`),
  CONSTRAINT `fk_logs_config` FOREIGN KEY (`id_config`) REFERENCES `config` (`id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Column Details for logs (21 columns):
-- id                        int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- id_config                 int                  -- INDEXED
-- count                     int                 
-- log_level                 enum('INFO','WARN','ERROR') -- INDEXED
-- status                    enum('SUCCESS','FAILED','RUNNING') -- INDEXED
-- create_time               datetime(6)          -- INDEXED, DEFAULT 'CURRENT_TIMESTAMP(6)', DEFAULT_GENERATED
-- update_time               datetime(6)          -- DEFAULT 'CURRENT_TIMESTAMP(6)', DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP(6)
-- created_by                varchar(255)        
-- destination_path          varchar(255)        
-- error_message             text                
-- location                  varchar(255)        
-- stack_trace               longtext            
-- session_id                varchar(36)         
-- operation_name            varchar(100)        
-- execution_time_ms         bigint              
-- memory_usage              decimal(10,2)       
-- source_path               varchar(255)        
-- file_size_bytes           bigint              
-- error_code                varchar(20)         
-- additional_info           longtext            
-- tags                      varchar(500)        

-- Indexes for logs:
CREATE INDEX `idx_id_config` ON `logs` (`id_config`);
CREATE INDEX `idx_log_level` ON `logs` (`log_level`);
CREATE INDEX `idx_status` ON `logs` (`status`);
CREATE INDEX `idx_create_time` ON `logs` (`create_time`);

-- End of logs table

-- =============================================
-- Table: etl_logs (28 columns)
-- Purpose: Enhanced ETL operation logging with performance metrics and session tracking
-- =============================================
DROP TABLE IF EXISTS `etl_logs`;

CREATE TABLE `etl_logs` (
  `log_id` int NOT NULL AUTO_INCREMENT,
  `operation_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `operation_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  `start_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `end_time` timestamp NULL DEFAULT NULL,
  `status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'STARTED',
  `records_processed` int DEFAULT '0',
  `records_inserted` int DEFAULT '0',
  `records_updated` int DEFAULT '0',
  `records_failed` int DEFAULT '0',
  `error_message` text COLLATE utf8mb4_unicode_ci,
  `execution_details` text COLLATE utf8mb4_unicode_ci,
  `created_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT 'system',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `session_id` varchar(36) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `parent_log_id` int DEFAULT NULL,
  `records_deleted` bigint DEFAULT NULL,
  `bytes_processed` bigint DEFAULT NULL,
  `error_code` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `stack_trace` longtext COLLATE utf8mb4_unicode_ci,
  `performance_metrics` longtext COLLATE utf8mb4_unicode_ci,
  `memory_usage_mb` decimal(10,2) DEFAULT NULL,
  `cpu_usage_percent` decimal(5,2) DEFAULT NULL,
  `source_file` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `target_table` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `server_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `process_id` int DEFAULT NULL,
  PRIMARY KEY (`log_id`),
  KEY `idx_operation_name` (`operation_name`),
  KEY `idx_operation_type` (`operation_type`),
  KEY `idx_start_time` (`start_time`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for etl_logs (28 columns):
-- log_id                    int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- operation_name            varchar(100)         -- INDEXED, NOT NULL
-- operation_type            varchar(20)          -- INDEXED, NOT NULL
-- start_time                timestamp            -- INDEXED, DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED
-- end_time                  timestamp           
-- status                    varchar(20)          -- INDEXED, NOT NULL, DEFAULT 'STARTED'
-- records_processed         int                  -- DEFAULT '0'
-- records_inserted          int                  -- DEFAULT '0'
-- records_updated           int                  -- DEFAULT '0'
-- records_failed            int                  -- DEFAULT '0'
-- error_message             text                
-- execution_details         text                
-- created_by                varchar(50)          -- DEFAULT 'system'
-- created_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED
-- updated_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED ON UPDATE CURRENT_TIMESTAMP
-- session_id                varchar(36)         
-- parent_log_id             int                 
-- records_deleted           bigint              
-- bytes_processed           bigint              
-- error_code                varchar(20)         
-- stack_trace               longtext            
-- performance_metrics       longtext            
-- memory_usage_mb           decimal(10,2)       
-- cpu_usage_percent         decimal(5,2)        
-- source_file               varchar(255)        
-- target_table              varchar(100)        
-- server_name               varchar(100)        
-- process_id                int                 

-- Indexes for etl_logs:
CREATE INDEX `idx_operation_name` ON `etl_logs` (`operation_name`);
CREATE INDEX `idx_operation_type` ON `etl_logs` (`operation_type`);
CREATE INDEX `idx_start_time` ON `etl_logs` (`start_time`);
CREATE INDEX `idx_status` ON `etl_logs` (`status`);

-- End of etl_logs table

-- =============================================
-- Table: data_quality_logs (24 columns)
-- Purpose: Comprehensive data quality monitoring with business impact assessment
-- =============================================
DROP TABLE IF EXISTS `data_quality_logs`;

CREATE TABLE `data_quality_logs` (
  `quality_id` int NOT NULL AUTO_INCREMENT,
  `table_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `quality_check_type` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `expected_value` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `actual_value` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `check_status` enum('PASS','FAIL','WARNING') COLLATE utf8mb4_unicode_ci NOT NULL,
  `check_details` text COLLATE utf8mb4_unicode_ci,
  `checked_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `checked_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT 'system',
  `log_id` int DEFAULT NULL,
  `column_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `quality_rule` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `severity_level` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `fix_suggestion` text COLLATE utf8mb4_unicode_ci,
  `business_impact` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `verification_method` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sample_data` longtext COLLATE utf8mb4_unicode_ci,
  `threshold_value` decimal(15,4) DEFAULT NULL,
  `variance_percentage` decimal(5,2) DEFAULT NULL,
  `record_count_affected` bigint DEFAULT NULL,
  `is_critical` tinyint(1) DEFAULT NULL,
  `resolution_status` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `resolved_at` timestamp NULL DEFAULT NULL,
  `resolved_by` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`quality_id`),
  KEY `idx_table_name` (`table_name`),
  KEY `idx_check_type` (`quality_check_type`),
  KEY `idx_check_status` (`check_status`)
) ENGINE=InnoDB AUTO_INCREMENT=73 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for data_quality_logs (24 columns):
-- quality_id                int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- table_name                varchar(100)         -- INDEXED, NOT NULL
-- quality_check_type        varchar(100)         -- INDEXED
-- expected_value            varchar(255)        
-- actual_value              varchar(255)        
-- check_status              enum('PASS','FAIL','WARNING') -- INDEXED, NOT NULL
-- check_details             text                
-- checked_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED
-- checked_by                varchar(50)          -- DEFAULT 'system'
-- log_id                    int                 
-- column_name               varchar(100)        
-- quality_rule              varchar(255)        
-- severity_level            varchar(20)         
-- fix_suggestion            text                
-- business_impact           varchar(50)         
-- verification_method       varchar(100)        
-- sample_data               longtext            
-- threshold_value           decimal(15,4)       
-- variance_percentage       decimal(5,2)        
-- record_count_affected     bigint              
-- is_critical               tinyint(1)          
-- resolution_status         varchar(30)         
-- resolved_at               timestamp           
-- resolved_by               varchar(50)         

-- Indexes for data_quality_logs:
CREATE INDEX `idx_table_name` ON `data_quality_logs` (`table_name`);
CREATE INDEX `idx_check_type` ON `data_quality_logs` (`quality_check_type`);
CREATE INDEX `idx_check_status` ON `data_quality_logs` (`check_status`);

-- End of data_quality_logs table

-- =============================================
-- Table: operation_tracking (26 columns)
-- Purpose: Advanced operation workflow tracking and dependency management
-- =============================================
DROP TABLE IF EXISTS `operation_tracking`;

CREATE TABLE `operation_tracking` (
  `track_id` int NOT NULL AUTO_INCREMENT,
  `session_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `operation_sequence` int NOT NULL,
  `parent_operation` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `current_operation` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `dependency_check` text COLLATE utf8mb4_unicode_ci,
  `execution_order` int DEFAULT NULL,
  `estimated_duration` int DEFAULT NULL,
  `actual_duration` int DEFAULT NULL,
  `memory_usage_mb` decimal(10,2) DEFAULT NULL,
  `cpu_usage_percent` decimal(5,2) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `operation_status` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dependency_status` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `priority_level` int DEFAULT NULL,
  `start_timestamp` timestamp NULL DEFAULT NULL,
  `end_timestamp` timestamp NULL DEFAULT NULL,
  `disk_io_mb` decimal(10,2) DEFAULT NULL,
  `network_io_mb` decimal(10,2) DEFAULT NULL,
  `thread_count` int DEFAULT NULL,
  `database_connections` int DEFAULT NULL,
  `error_count` int DEFAULT NULL,
  `warning_count` int DEFAULT NULL,
  `retry_count` int DEFAULT NULL,
  `checkpoint_data` longtext COLLATE utf8mb4_unicode_ci,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`track_id`),
  KEY `idx_session_id` (`session_id`),
  KEY `idx_operation_sequence` (`operation_sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Column Details for operation_tracking (26 columns):
-- track_id                  int                  -- PRIMARY KEY, NOT NULL, AUTO_INCREMENT
-- session_id                varchar(36)          -- INDEXED, NOT NULL
-- operation_sequence        int                  -- INDEXED, NOT NULL
-- parent_operation          varchar(100)        
-- current_operation         varchar(100)         -- NOT NULL
-- dependency_check          text                
-- execution_order           int                 
-- estimated_duration        int                 
-- actual_duration           int                 
-- memory_usage_mb           decimal(10,2)       
-- cpu_usage_percent         decimal(5,2)        
-- created_at                timestamp            -- DEFAULT CURRENT_TIMESTAMP, DEFAULT_GENERATED
-- operation_status          varchar(30)         
-- dependency_status         varchar(30)         
-- priority_level            int                 
-- start_timestamp           timestamp           
-- end_timestamp             timestamp           
-- disk_io_mb                decimal(10,2)       
-- network_io_mb             decimal(10,2)       
-- thread_count              int                 
-- database_connections      int                 
-- error_count               int                 
-- warning_count             int                 
-- retry_count               int                 
-- checkpoint_data           longtext            
-- updated_at                timestamp           

-- Indexes for operation_tracking:
CREATE INDEX `idx_session_id` ON `operation_tracking` (`session_id`);
CREATE INDEX `idx_operation_sequence` ON `operation_tracking` (`operation_sequence`);

-- End of operation_tracking table

-- =============================================
-- Enhanced Features Summary
-- =============================================

-- Enhanced Control Database includes:
-- 1. config table (57 columns): 
--    - Crawler configuration (source_url, target_books, user_agent)
--    - Database settings (staging_host, dw_host, connection parameters)
--    - ETL tuning (batch_size, parallel_processes, error_threshold)
--    - Monitoring setup (alert_on_failure, performance_threshold)

-- 2. etl_logs table (28 columns):
--    - Session tracking (session_id, parent_log_id)
--    - Performance metrics (memory_usage_mb, cpu_usage_percent)
--    - Complete record tracking (records_deleted, bytes_processed)
--    - Error management (error_code, stack_trace)

-- 3. data_quality_logs table (24 columns):
--    - Column-level validation (column_name, quality_rule)
--    - Severity classification (severity_level, is_critical)
--    - Business impact assessment (business_impact, fix_suggestion)
--    - Resolution workflow (resolution_status, resolved_at)

-- 4. logs table (21 columns):
--    - Enhanced with session correlation (session_id)
--    - Performance tracking (execution_time_ms, memory_usage)
--    - File operations (source_path, file_size_bytes)

-- 5. operation_tracking table (26 columns):
--    - Workflow management (operation_sequence, parent_operation)
--    - Resource monitoring (disk_io_mb, network_io_mb)
--    - Status tracking (operation_status, dependency_status)
--    - Checkpoint management (checkpoint_data, priority_level)

-- Total Enhancement: +77 columns across 5 tables (156 total columns)

-- =============================================
-- Re-enable foreign key checks
-- =============================================
SET FOREIGN_KEY_CHECKS = 1;

-- =============================================
-- Post-Creation Verification Queries
-- =============================================

-- Verify all tables created:
-- SHOW TABLES;

-- Check column counts:
-- SELECT TABLE_NAME, 
--        COUNT(*) as COLUMN_COUNT
-- FROM information_schema.COLUMNS 
-- WHERE TABLE_SCHEMA = 'fahasa_control'
-- GROUP BY TABLE_NAME
-- ORDER BY TABLE_NAME;

-- Verify total column count (should be 156):
-- SELECT COUNT(*) as TOTAL_COLUMNS
-- FROM information_schema.COLUMNS 
-- WHERE TABLE_SCHEMA = 'fahasa_control';

-- =============================================
-- End of Enhanced Control Database Schema
-- =============================================
