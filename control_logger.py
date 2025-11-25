#!/usr/bin/env python3
"""
Control Logging Integration
Wrapper class to add control logging to crawl and ETL operations
"""

import mysql.connector
import json
from datetime import datetime
import traceback

class ControlLogger:
    """Control logging for crawl and ETL operations"""
    
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'user': 'root', 
            'password': '123456',
            'database': 'fahasa_control',
            'charset': 'utf8mb4'
        }
        self.current_log_id = None
    
    def get_connection(self):
        """Get control database connection"""
        try:
            return mysql.connector.connect(**self.config)
        except Exception as e:
            print(f"Warning: Could not connect to control database: {e}")
            return None
    
    def start_operation(self, operation_name, operation_type, details=None):
        """Start logging an operation"""
        try:
            conn = self.get_connection()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            details_json = json.dumps(details) if details else '{}'
            
            cursor.execute("""
                INSERT INTO etl_logs (
                    operation_name, operation_type, status,
                    execution_details, start_time
                ) VALUES (%s, %s, %s, %s, NOW())
            """, (operation_name, operation_type, 'STARTED', details_json))
            
            log_id = cursor.lastrowid
            self.current_log_id = log_id
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Control Log Started: {operation_name} (ID: {log_id})")
            return log_id
            
        except Exception as e:
            print(f"Warning: Could not start control logging: {e}")
            return None
    
    def end_operation(self, log_id=None, status='SUCCESS', 
                     records_processed=0, records_inserted=0, 
                     records_updated=0, records_failed=0, error_message=None):
        """End logging an operation"""
        try:
            if not log_id:
                log_id = self.current_log_id
            
            if not log_id:
                return False
                
            conn = self.get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE etl_logs 
                SET status = %s, end_time = NOW(),
                    records_processed = %s, records_inserted = %s,
                    records_updated = %s, records_failed = %s,
                    error_message = %s, updated_at = NOW()
                WHERE log_id = %s
            """, (status, records_processed, records_inserted, 
                  records_updated, records_failed, error_message, log_id))
            
            # Get operation details for reporting
            cursor.execute("""
                SELECT operation_name, 
                       TIMESTAMPDIFF(SECOND, start_time, end_time) as duration
                FROM etl_logs WHERE log_id = %s
            """, (log_id,))
            
            result = cursor.fetchone()
            if result:
                operation_name, duration = result
                print(f"Control Log Ended: {operation_name} - {status} ({duration}s)")
                print(f"   Records: {records_processed:,} processed, {records_inserted:,} inserted")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.current_log_id = None
            return True
            
        except Exception as e:
            print(f"Warning: Could not end control logging: {e}")
            return False
    
    def log_quality_check(self, table_name, check_type, expected, actual, status, details=None):
        """Log data quality check"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO data_quality_logs (
                    table_name, quality_check_type, expected_value,
                    actual_value, check_status, check_details
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (table_name, check_type, str(expected), str(actual), status, details))
            
            quality_id = cursor.lastrowid
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"   Quality Check: {table_name} - {check_type} - {status}")
            return quality_id
            
        except Exception as e:
            print(f"Warning: Could not log quality check: {e}")
            return False
    
    def get_operation_stats(self, operation_type=None, days=7):
        """Get operation statistics"""
        try:
            conn = self.get_connection()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            where_clause = ""
            params = [days]
            
            if operation_type:
                where_clause = "AND operation_type = %s"
                params.append(operation_type)
            
            cursor.execute(f"""
                SELECT 
                    operation_type,
                    COUNT(*) as total_operations,
                    SUM(records_processed) as total_records,
                    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_duration,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count
                FROM etl_logs 
                WHERE start_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                {where_clause}
                GROUP BY operation_type
                ORDER BY total_operations DESC
            """, params)
            
            stats = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Exception as e:
            print(f"Warning: Could not get operation stats: {e}")
            return None

# Context managers for easy usage
class CrawlLogger:
    """Context manager for crawl operations"""
    
    def __init__(self, operation_name, details=None):
        self.logger = ControlLogger()
        self.operation_name = operation_name
        self.details = details
        self.log_id = None
        self.records_processed = 0
        self.records_inserted = 0
        self.error_message = None
    
    def __enter__(self):
        self.log_id = self.logger.start_operation(
            self.operation_name, 'CRAWL', self.details
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            status = 'FAILED'
            self.error_message = str(exc_val)
        else:
            status = 'SUCCESS'
        
        self.logger.end_operation(
            self.log_id, status, 
            self.records_processed, self.records_inserted,
            error_message=self.error_message
        )
    
    def update_progress(self, processed=0, inserted=0):
        """Update progress counters"""
        self.records_processed = processed
        self.records_inserted = inserted

class ETLLogger:
    """Context manager for ETL operations"""
    
    def __init__(self, operation_name, details=None):
        self.logger = ControlLogger()
        self.operation_name = operation_name
        self.details = details
        self.log_id = None
        self.records_processed = 0
        self.records_inserted = 0
        self.records_updated = 0
        self.error_message = None
    
    def __enter__(self):
        self.log_id = self.logger.start_operation(
            self.operation_name, 'ETL', self.details
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            status = 'FAILED'
            self.error_message = str(exc_val)
        else:
            status = 'SUCCESS'
        
        self.logger.end_operation(
            self.log_id, status,
            self.records_processed, self.records_inserted, self.records_updated,
            error_message=self.error_message
        )
    
    def update_progress(self, processed=0, inserted=0, updated=0):
        """Update progress counters"""
        self.records_processed = processed
        self.records_inserted = inserted
        self.records_updated = updated

# Example usage functions
def demo_crawl_with_logging():
    """Demo crawl operation with control logging"""
    print("DEMO: Crawl with Control Logging")
    print("-" * 40)
    
    with CrawlLogger('demo_crawl_operation', {'source': 'fahasa', 'mode': 'demo'}) as crawl_log:
        # Simulate crawl work
        import time
        
        print("   Starting crawl simulation...")
        time.sleep(1)
        
        crawl_log.update_progress(processed=50, inserted=45)
        print("   Processed 50 books, inserted 45...")
        
        time.sleep(1)
        
        crawl_log.update_progress(processed=100, inserted=95)
        print("   Processed 100 books, inserted 95...")
        
        # Quality check
        crawl_log.logger.log_quality_check(
            'staging_books', 'COUNT', '>=95', '95', 'PASS',
            'Crawl resulted in expected number of records'
        )
    
    print("   Crawl operation logged successfully")

def demo_etl_with_logging():
    """Demo ETL operation with control logging"""
    print("\nDEMO: ETL with Control Logging")
    print("-" * 40)
    
    with ETLLogger('demo_etl_operation', {'procedures': 18, 'mode': 'demo'}) as etl_log:
        # Simulate ETL work
        import time
        
        print("   Starting ETL simulation...")
        time.sleep(1)
        
        etl_log.update_progress(processed=50000, inserted=48000, updated=2000)
        print("   Processed 50K records...")
        
        time.sleep(1)
        
        etl_log.update_progress(processed=100000, inserted=95000, updated=5000)
        print("   Processed 100K records...")
        
        # Quality checks
        etl_log.logger.log_quality_check(
            'fact_book_sales', 'COUNT', '>0', '100000', 'PASS',
            'ETL populated fact table successfully'
        )
    
    print("   ETL operation logged successfully")

def show_operation_summary():
    """Show operation summary"""
    print("\nOPERATION SUMMARY")
    print("-" * 40)
    
    logger = ControlLogger()
    stats = logger.get_operation_stats()
    
    if stats:
        for stat in stats:
            op_type, total_ops, total_records, avg_duration, success_count, failed_count = stat
            success_rate = (success_count / total_ops * 100) if total_ops > 0 else 0
            avg_duration = avg_duration if avg_duration else 0
            
            print(f"   {op_type}:")
            print(f"      • Total Operations: {total_ops}")
            print(f"      • Total Records: {total_records:,}")
            print(f"      • Success Rate: {success_rate:.1f}%")
            print(f"      • Avg Duration: {avg_duration:.1f}s")
    else:
        print("   No statistics available")

def main():
    """Main demo function"""
    print("CONTROL LOGGING INTEGRATION DEMO")
    print(f"Started: {datetime.now()}")
    print("=" * 50)
    
    # Demo both logging types
    demo_crawl_with_logging()
    demo_etl_with_logging()
    
    # Show summary
    show_operation_summary()
    
    print("\n" + "=" * 50)
    print("CONTROL LOGGING INTEGRATION READY!")
    print("Can be used in fahasa_bulk_scraper.py")
    print("Can be used in daily_etl.py")
    print("=" * 50)

if __name__ == "__main__":
    main()