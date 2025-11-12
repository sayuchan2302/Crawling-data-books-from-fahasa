"""
Staging Control Module
Quản lý ETL batch tracking, data quality validation và monitoring
"""

import psycopg2
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class StagingController:
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
    
    def get_connection(self):
        return psycopg2.connect(**self.connection_params)
    
    def start_batch(self, source_type: str, source_identifier: str = None, 
                   created_by: str = 'system', notes: str = None) -> int:
        """
        Bắt đầu một ETL batch mới
        Returns: batch_id
        """
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            insert_sql = """
                INSERT INTO staging_control_log 
                (source_type, source_identifier, created_by, notes)
                VALUES (%s, %s, %s, %s)
                RETURNING batch_id
            """
            cur.execute(insert_sql, (source_type, source_identifier, created_by, notes))
            batch_id = cur.fetchone()[0]
            conn.commit()
            
            print(f"✅ Started batch {batch_id} for {source_type}")
            return batch_id
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error starting batch: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def update_batch_progress(self, batch_id: int, records_extracted: int = None,
                            records_loaded: int = None, records_rejected: int = None):
        """Cập nhật tiến độ batch"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            update_fields = []
            params = []
            
            if records_extracted is not None:
                update_fields.append("records_extracted = %s")
                params.append(records_extracted)
            if records_loaded is not None:
                update_fields.append("records_loaded = %s")
                params.append(records_loaded)
            if records_rejected is not None:
                update_fields.append("records_rejected = %s")
                params.append(records_rejected)
            
            if update_fields:
                update_sql = f"""
                    UPDATE staging_control_log 
                    SET {', '.join(update_fields)}
                    WHERE batch_id = %s
                """
                params.append(batch_id)
                cur.execute(update_sql, params)
                conn.commit()
                
        except Exception as e:
            conn.rollback()
            print(f"❌ Error updating batch progress: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def finish_batch(self, batch_id: int, status: str = 'SUCCESS', 
                    error_message: str = None, error_count: int = 0):
        """Kết thúc batch với status và metrics"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            # Calculate duration and performance metrics
            update_sql = """
                UPDATE staging_control_log 
                SET 
                    end_time = CURRENT_TIMESTAMP,
                    status = %s,
                    error_message = %s,
                    error_count = %s,
                    duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)),
                    records_per_second = CASE 
                        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)) > 0 
                        THEN records_loaded / EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time))
                        ELSE 0 
                    END
                WHERE batch_id = %s
            """
            cur.execute(update_sql, (status, error_message, error_count, batch_id))
            conn.commit()
            
            print(f"✅ Finished batch {batch_id} with status: {status}")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error finishing batch: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def run_data_quality_checks(self, batch_id: int) -> List[Dict]:
        """Chạy tất cả data quality checks cho batch"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            # Get active validation rules
            cur.execute("""
                SELECT rule_id, rule_name, rule_description, rule_query, 
                       warning_threshold, critical_threshold, is_blocking
                FROM staging_validation_rules 
                WHERE is_active = TRUE
                ORDER BY is_blocking DESC, rule_name
            """)
            
            rules = cur.fetchall()
            results = []
            
            for rule in rules:
                rule_id, rule_name, rule_desc, rule_query, warn_threshold, crit_threshold, is_blocking = rule
                
                # Execute validation query
                try:
                    cur.execute(rule_query, (batch_id,))
                    failed_count = cur.fetchone()[0]
                    
                    # Get total records for this batch
                    cur.execute("SELECT COUNT(*) FROM staging_books WHERE batch_id = %s", (batch_id,))
                    total_count = cur.fetchone()[0]
                    
                    if total_count == 0:
                        failure_rate = 0.0
                        status = 'PASS'
                    else:
                        failure_rate = (failed_count / total_count) * 100
                        
                        if failure_rate >= crit_threshold:
                            status = 'CRITICAL'
                        elif failure_rate >= warn_threshold:
                            status = 'WARNING'
                        else:
                            status = 'PASS'
                    
                    passed_count = total_count - failed_count
                    
                    # Insert quality check result
                    quality_sql = """
                        INSERT INTO staging_data_quality 
                        (batch_id, check_name, check_description, check_query,
                         total_records, passed_records, failed_records, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING quality_id
                    """
                    cur.execute(quality_sql, (
                        batch_id, rule_name, rule_desc, rule_query,
                        total_count, passed_count, failed_count, status
                    ))
                    quality_id = cur.fetchone()[0]
                    
                    result = {
                        'quality_id': quality_id,
                        'rule_name': rule_name,
                        'status': status,
                        'total_records': total_count,
                        'failed_records': failed_count,
                        'failure_rate': round(failure_rate, 2),
                        'is_blocking': is_blocking
                    }
                    results.append(result)
                    
                    print(f"📊 {rule_name}: {status} - {failed_count}/{total_count} failed ({failure_rate:.1f}%)")
                    
                    # Stop if blocking rule fails critically
                    if is_blocking and status == 'CRITICAL':
                        print(f"🚫 BLOCKING rule {rule_name} failed critically! Stopping quality checks.")
                        self.finish_batch(batch_id, 'FAILED', f"Blocking quality check failed: {rule_name}")
                        break
                        
                except Exception as rule_error:
                    print(f"❌ Error running rule {rule_name}: {rule_error}")
                    # Log failed quality check
                    quality_sql = """
                        INSERT INTO staging_data_quality 
                        (batch_id, check_name, check_description, status, total_records, passed_records, failed_records)
                        VALUES (%s, %s, %s, 'FAILED', 0, 0, 0)
                    """
                    cur.execute(quality_sql, (batch_id, rule_name, f"Error: {rule_error}"))
            
            conn.commit()
            return results
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error running data quality checks: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def get_batch_summary(self, batch_id: int) -> Dict:
        """Lấy summary của batch"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            # Get batch info
            cur.execute("""
                SELECT batch_id, batch_date, start_time, end_time, status,
                       source_type, records_extracted, records_loaded, 
                       records_rejected, duration_seconds, records_per_second,
                       error_message, error_count
                FROM staging_control_log 
                WHERE batch_id = %s
            """, (batch_id,))
            
            batch_info = cur.fetchone()
            if not batch_info:
                return None
            
            # Get quality checks summary
            cur.execute("""
                SELECT status, COUNT(*) as count
                FROM staging_data_quality 
                WHERE batch_id = %s
                GROUP BY status
            """, (batch_id,))
            
            quality_summary = {row[0]: row[1] for row in cur.fetchall()}
            
            return {
                'batch_id': batch_info[0],
                'batch_date': batch_info[1],
                'start_time': batch_info[2],
                'end_time': batch_info[3],
                'status': batch_info[4],
                'source_type': batch_info[5],
                'records_extracted': batch_info[6],
                'records_loaded': batch_info[7],
                'records_rejected': batch_info[8],
                'duration_seconds': batch_info[9],
                'records_per_second': batch_info[10],
                'error_message': batch_info[11],
                'error_count': batch_info[12],
                'quality_checks': quality_summary
            }
            
        finally:
            cur.close()
            conn.close()


class StagingDataValidator:
    """Dedicated class for data validation operations"""
    
    def __init__(self, controller: StagingController):
        self.controller = controller
    
    def validate_batch_data(self, batch_id: int) -> bool:
        """
        Chạy validation và trả về True nếu dữ liệu OK để load vào DW
        """
        results = self.controller.run_data_quality_checks(batch_id)
        
        # Check if any blocking rules failed
        blocking_failures = [r for r in results if r['is_blocking'] and r['status'] == 'CRITICAL']
        
        if blocking_failures:
            print(f"❌ Batch {batch_id} FAILED validation - {len(blocking_failures)} blocking issues")
            return False
        
        # Count warnings and criticals
        warnings = len([r for r in results if r['status'] == 'WARNING'])
        criticals = len([r for r in results if r['status'] == 'CRITICAL'])
        
        if criticals > 0:
            print(f"⚠️  Batch {batch_id} has {criticals} critical issues (non-blocking)")
        
        if warnings > 0:
            print(f"⚠️  Batch {batch_id} has {warnings} warnings")
        
        print(f"✅ Batch {batch_id} PASSED validation")
        return True
    
    def mark_invalid_records(self, batch_id: int):
        """Đánh dấu records không hợp lệ trong staging_books"""
        conn = self.controller.get_connection()
        cur = conn.cursor()
        
        try:
            # Mark records with validation errors
            validation_updates = [
                ("title IS NULL OR title = ''", "Missing title"),
                ("original_price < 0 OR discount_price < 0", "Invalid price"),
                ("rating < 0 OR rating > 5", "Invalid rating"),
                ("url NOT LIKE '%fahasa%' AND url IS NOT NULL", "Invalid URL format")
            ]
            
            for condition, error_msg in validation_updates:
                update_sql = f"""
                    UPDATE staging_books 
                    SET record_status = 'ERROR',
                        validation_errors = COALESCE(validation_errors, '[]'::jsonb) || 
                                          jsonb_build_array(%s)
                    WHERE batch_id = %s AND ({condition})
                """
                cur.execute(update_sql, (error_msg, batch_id))
            
            conn.commit()
            print(f"✅ Marked invalid records for batch {batch_id}")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error marking invalid records: {e}")
            raise
        finally:
            cur.close()
            conn.close()


# Example usage functions
def example_usage():
    """Ví dụ cách sử dụng StagingController"""
    
    # Database connection
    connection_params = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'your_password',
        'dbname': 'fahasa_staging'
    }
    
    # Initialize controller
    controller = StagingController(connection_params)
    validator = StagingDataValidator(controller)
    
    # Start a batch
    batch_id = controller.start_batch(
        source_type='CRAWLER',
        source_identifier='fahasa_bulk_scraper.py',
        notes='Daily crawl session'
    )
    
    try:
        # Simulate data loading
        controller.update_batch_progress(batch_id, records_extracted=1000)
        
        # ... actual data loading logic here ...
        
        controller.update_batch_progress(batch_id, records_loaded=950, records_rejected=50)
        
        # Run data quality validation
        if validator.validate_batch_data(batch_id):
            validator.mark_invalid_records(batch_id)
            controller.finish_batch(batch_id, 'SUCCESS')
        else:
            controller.finish_batch(batch_id, 'FAILED', 'Data quality validation failed')
        
        # Get summary
        summary = controller.get_batch_summary(batch_id)
        print(f"📋 Batch Summary: {summary}")
        
    except Exception as e:
        controller.finish_batch(batch_id, 'FAILED', str(e), 1)
        raise


if __name__ == '__main__':
    example_usage()