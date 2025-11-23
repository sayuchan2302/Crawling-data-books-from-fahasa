#!/usr/bin/env python3
"""
Control Logger Utility - Ghi log vào fahasa_control database
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime
import json
import traceback
from enum import Enum

class LogLevel(Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

class LogStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RUNNING = "RUNNING"

class ControlLogger:
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'database': 'fahasa_control',
            'user': 'root',
            'password': '123456'
        }
        
    def get_connection(self):
        """Tạo kết nối control database"""
        try:
            return mysql.connector.connect(**self.config)
        except Error as e:
            print(f"❌ Control DB connection error: {e}")
            return None
    
    def create_config(self, config_data):
        """Tạo config mới trong bảng config"""
        conn = self.get_connection()
        if not conn:
            return None
            
        try:
            cursor = conn.cursor()
            
            query = """
            INSERT INTO config (
                file_name, file_path, file_encoding, crawl_frequency, data_size,
                retry_count, timeout, dw_source_port, staging_source_port,
                source_path, destination_path, backup_path, file_type, delimiter,
                dw_source_host, dw_source_password, dw_source_username,
                staging_source_host, staging_source_password, staging_source_username,
                columns, tables, note, notification_emails, created_by, version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                config_data.get('file_name', ''),
                config_data.get('file_path', ''),
                config_data.get('file_encoding', 'utf-8'),
                config_data.get('crawl_frequency', 60),
                config_data.get('data_size', 0),
                config_data.get('retry_count', 3),
                config_data.get('timeout', 30),
                config_data.get('dw_source_port', 3306),
                config_data.get('staging_source_port', 3306),
                config_data.get('source_path', ''),
                config_data.get('destination_path', ''),
                config_data.get('backup_path', ''),
                config_data.get('file_type', 'csv'),
                config_data.get('delimiter', ','),
                config_data.get('dw_source_host', 'localhost'),
                config_data.get('dw_source_password', '123456'),
                config_data.get('dw_source_username', 'root'),
                config_data.get('staging_source_host', 'localhost'),
                config_data.get('staging_source_password', '123456'),
                config_data.get('staging_source_username', 'root'),
                json.dumps(config_data.get('columns', [])) if config_data.get('columns') else '',
                config_data.get('tables', ''),
                config_data.get('note', ''),
                config_data.get('notification_emails', ''),
                config_data.get('created_by', 'system'),
                config_data.get('version', '1.0')
            )
            
            cursor.execute(query, values)
            conn.commit()
            config_id = cursor.lastrowid
            cursor.close()
            conn.close()
            
            print(f"✅ Created config ID: {config_id}")
            return config_id
            
        except Error as e:
            print(f"❌ Error creating config: {e}")
            if conn:
                conn.close()
            return None
    
    def log_operation(self, operation_type, status=LogStatus.RUNNING, log_level=LogLevel.INFO, 
                     count=0, destination_path='', error_message='', location='', 
                     created_by='system', config_id=None):
        """Ghi log operation vào bảng logs"""
        conn = self.get_connection()
        if not conn:
            return None
            
        try:
            cursor = conn.cursor()
            
            # Nếu có error, lấy stack trace
            stack_trace = ''
            if error_message and log_level == LogLevel.ERROR:
                stack_trace = traceback.format_exc()
            
            query = """
            INSERT INTO logs (
                id_config, count, log_level, status, destination_path, 
                error_message, location, stack_trace, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                config_id,
                count,
                log_level.value,
                status.value,
                destination_path,
                error_message,
                f"{operation_type} - {location}",
                stack_trace,
                created_by
            )
            
            cursor.execute(query, values)
            conn.commit()
            log_id = cursor.lastrowid
            cursor.close()
            conn.close()
            
            # Print log để user thấy
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status_emoji = {"SUCCESS": "✅", "FAILED": "❌", "RUNNING": "🔄"}
            level_emoji = {"INFO": "ℹ️", "WARN": "⚠️", "ERROR": "💥"}
            
            print(f"{level_emoji.get(log_level.value, 'ℹ️')} {timestamp} [{log_level.value}] {status_emoji.get(status.value, '🔄')} {operation_type}")
            if count > 0:
                print(f"   📊 Records: {count:,}")
            if destination_path:
                print(f"   📁 Path: {destination_path}")
            if error_message:
                print(f"   💬 Message: {error_message}")
                
            return log_id
            
        except Error as e:
            print(f"❌ Error logging operation: {e}")
            if conn:
                conn.close()
            return None
    
    def update_log_status(self, log_id, status, count=None, error_message=''):
        """Update trạng thái của log entry"""
        conn = self.get_connection()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            
            if count is not None:
                query = """
                UPDATE logs 
                SET status = %s, count = %s, error_message = %s, 
                    update_time = CURRENT_TIMESTAMP(6)
                WHERE id = %s
                """
                values = (status.value, count, error_message, log_id)
            else:
                query = """
                UPDATE logs 
                SET status = %s, error_message = %s, 
                    update_time = CURRENT_TIMESTAMP(6)
                WHERE id = %s
                """
                values = (status.value, error_message, log_id)
            
            cursor.execute(query, values)
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Error as e:
            print(f"❌ Error updating log: {e}")
            if conn:
                conn.close()
            return False
    
    def log_crawl_start(self, target_books, config_id=None):
        """Log bắt đầu crawl"""
        return self.log_operation(
            operation_type="CRAWL_START",
            status=LogStatus.RUNNING,
            log_level=LogLevel.INFO,
            location="fahasa_bulk_scraper.py",
            error_message=f"Starting crawl for {target_books} books",
            config_id=config_id
        )
    
    def log_crawl_success(self, log_id, books_count, csv_path, json_path):
        """Log crawl thành công"""
        self.update_log_status(
            log_id=log_id,
            status=LogStatus.SUCCESS,
            count=books_count,
            error_message=f"Crawl completed. Files: {csv_path}, {json_path}"
        )
    
    def log_crawl_error(self, log_id, error_message):
        """Log crawl lỗi"""
        self.update_log_status(
            log_id=log_id,
            status=LogStatus.FAILED,
            error_message=str(error_message)
        )
    
    def log_etl_start(self, stage, source_table, target_table, config_id=None):
        """Log bắt đầu ETL"""
        return self.log_operation(
            operation_type=f"ETL_{stage.upper()}",
            status=LogStatus.RUNNING,
            log_level=LogLevel.INFO,
            location="etl_process.py",
            error_message=f"ETL {stage}: {source_table} → {target_table}",
            config_id=config_id
        )
    
    def log_etl_success(self, log_id, records_processed):
        """Log ETL thành công"""
        self.update_log_status(
            log_id=log_id,
            status=LogStatus.SUCCESS,
            count=records_processed
        )
    
    def log_etl_error(self, log_id, error_message):
        """Log ETL lỗi"""
        self.update_log_status(
            log_id=log_id,
            status=LogStatus.FAILED,
            error_message=str(error_message)
        )

# Global logger instance
logger = ControlLogger()

# Helper functions for easy use
def log_info(message, operation="GENERAL", count=0, path=''):
    """Quick info logging"""
    return logger.log_operation(
        operation_type=operation,
        status=LogStatus.SUCCESS,
        log_level=LogLevel.INFO,
        count=count,
        destination_path=path,
        error_message=message
    )

def log_error(message, operation="GENERAL", location=''):
    """Quick error logging"""
    return logger.log_operation(
        operation_type=operation,
        status=LogStatus.FAILED,
        log_level=LogLevel.ERROR,
        error_message=message,
        location=location
    )

def log_warning(message, operation="GENERAL"):
    """Quick warning logging"""
    return logger.log_operation(
        operation_type=operation,
        status=LogStatus.SUCCESS,
        log_level=LogLevel.WARN,
        error_message=message
    )