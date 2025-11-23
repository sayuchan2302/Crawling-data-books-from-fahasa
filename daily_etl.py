#!/usr/bin/env python3
"""
Daily ETL Transform Pipeline - Chạy tất cả stored procedures
Staging → DW → DataMart → BI Aggregates
"""

import mysql.connector
from mysql.connector import Error
import sys
import os
from datetime import datetime

# Add utils path for control logging
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
try:
    from control_logger import logger, LogStatus, LogLevel
except ImportError:
    print("⚠️ Control logger not available - running without logging")
    logger = None

class FahasaETL:
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'charset': 'utf8mb4',
            'autocommit': True
        }
    
    def connect_db(self, database):
        """Connect to specific database"""
        try:
            config = self.config.copy()
            config['database'] = database
            return mysql.connector.connect(**config)
        except Error as e:
            print(f"❌ Connection error to {database}: {e}")
            return None
    
    def run_transform(self):
        """Run complete ETL transform pipeline"""
        etl_log_id = None
        
        try:
            print("🚀 FAHASA ETL TRANSFORM PIPELINE")
            print("=" * 60)
            print(f"📅 Start Time: {datetime.now()}")
            print("=" * 60)
            
            # Log ETL start
            if logger:
                etl_log_id = logger.log_operation(
                    operation_type="FULL_ETL_TRANSFORM",
                    status=LogStatus.RUNNING,
                    log_level=LogLevel.INFO,
                    location="daily_etl.py",
                    error_message="Starting full ETL transform pipeline"
                )
            
            # Step 1: Transform Staging → DW
            print("📊 STEP 1: STAGING → DATA WAREHOUSE")
            print("-" * 50)
            dw_success = self.run_dw_transform()
            
            if not dw_success:
                raise Exception("DW Transform failed")
            
            # Step 2: Transform DW → DataMart
            print("\n📈 STEP 2: DATA WAREHOUSE → DATAMART")
            print("-" * 50)
            mart_success = self.run_datamart_transform()
            
            if not mart_success:
                raise Exception("DataMart Transform failed")
            
            # Step 3: Generate BI Aggregates
            print("\n🎯 STEP 3: GENERATE BI AGGREGATES")
            print("-" * 50)
            bi_success = self.run_bi_aggregates()
            
            # Success logging
            if logger and etl_log_id:
                logger.update_log_status(etl_log_id, LogStatus.SUCCESS, error_message="ETL Transform completed successfully")
            
            print("\n🎉 ETL TRANSFORM PIPELINE HOÀN TẤT!")
            print("✅ Staging → DW → DataMart → BI Aggregates")
            print(f"📅 End Time: {datetime.now()}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ ETL Transform failed: {e}")
            if logger and etl_log_id:
                logger.update_log_status(etl_log_id, LogStatus.FAILED, error_message=str(e))
            return False
    
    def run_dw_transform(self):
        """Run Data Warehouse transformation procedures"""
        dw_conn = self.connect_db('fahasa_dw')
        if not dw_conn:
            return False
            
        cursor = dw_conn.cursor()
        
        try:
            print("   🔄 Running DW transformation stored procedures...")
            
            # List of key DW procedures to run
            dw_procedures = [
                'sp_simple_load_publishers',
                'sp_load_author_dim',
                'sp_load_category_dim', 
                'sp_load_product_dim',
                'sp_simple_load_product_author_bridge',
                'sp_simple_load_product_category_bridge',
                'sp_simple_load_product_publisher_bridge',
                'sp_complete_load_fact_metrics'
            ]
            
            successful_procs = 0
            
            for proc in dw_procedures:
                try:
                    print(f"      🔄 {proc}...")
                    cursor.callproc(proc)
                    
                    # Consume results to avoid "Unread result found"
                    for result in cursor.stored_results():
                        result.fetchall()
                    
                    print(f"      ✅ {proc} completed")
                    successful_procs += 1
                    
                except Error as e:
                    print(f"      ⚠️ {proc} failed: {e}")
                    continue
            
            print(f"\n   📊 DW Transform Summary: {successful_procs}/{len(dw_procedures)} procedures successful")
            
            # Show DW statistics
            self.show_dw_stats(cursor)
            
            return successful_procs > 0
            
        except Exception as e:
            print(f"   ❌ DW Transform error: {e}")
            return False
        finally:
            cursor.close()
            dw_conn.close()
    
    def run_datamart_transform(self):
        """Run DataMart transformation procedures"""
        mart_conn = self.connect_db('fahasa_datamart')
        if not mart_conn:
            return False
            
        cursor = mart_conn.cursor()
        
        try:
            print("   🔄 Running DataMart transformation procedures...")
            
            # List of DataMart procedures
            mart_procedures = [
                'sp_populate_mart_daily_sales',
                'sp_populate_mart_category_performance',
                'sp_populate_mart_author_insights',
                'sp_populate_mart_publisher_performance',
                'sp_populate_mart_product_flat'
            ]
            
            successful_procs = 0
            
            for proc in mart_procedures:
                try:
                    print(f"      🔄 {proc}...")
                    cursor.callproc(proc)
                    
                    # Consume results
                    for result in cursor.stored_results():
                        result.fetchall()
                    
                    print(f"      ✅ {proc} completed")
                    successful_procs += 1
                    
                except Error as e:
                    print(f"      ⚠️ {proc} failed: {e}")
                    continue
            
            print(f"\n   📈 DataMart Transform Summary: {successful_procs}/{len(mart_procedures)} procedures successful")
            
            # Show DataMart statistics
            self.show_mart_stats(cursor)
            
            return successful_procs > 0
            
        except Exception as e:
            print(f"   ❌ DataMart Transform error: {e}")
            return False
        finally:
            cursor.close()
            mart_conn.close()
    
    def run_bi_aggregates(self):
        """Run BI aggregate procedures"""
        dw_conn = self.connect_db('fahasa_dw')
        if not dw_conn:
            return False
            
        cursor = dw_conn.cursor()
        
        try:
            print("   🔄 Running BI aggregate procedures...")
            
            # List of BI aggregate procedures
            bi_procedures = [
                'sp_populate_author_performance',
                'sp_populate_publisher_revenue',
                'sp_populate_category_sales',
                'sp_populate_price_range',
                'sp_populate_rating_distribution'
            ]
            
            successful_procs = 0
            
            for proc in bi_procedures:
                try:
                    print(f"      🔄 {proc}...")
                    cursor.callproc(proc)
                    
                    # Consume results
                    for result in cursor.stored_results():
                        result.fetchall()
                    
                    print(f"      ✅ {proc} completed")
                    successful_procs += 1
                    
                except Error as e:
                    print(f"      ⚠️ {proc} failed: {e}")
                    continue
            
            print(f"\n   🎯 BI Aggregates Summary: {successful_procs}/{len(bi_procedures)} procedures successful")
            
            return True  # BI aggregates are optional, so always return success
            
        except Exception as e:
            print(f"   ❌ BI Aggregates error: {e}")
            return False
        finally:
            cursor.close()
            dw_conn.close()
    
    def show_dw_stats(self, cursor):
        """Show Data Warehouse statistics"""
        try:
            cursor.execute("""
                SELECT 'Products' as table_name, COUNT(*) as count FROM product_dim
                UNION ALL SELECT 'Authors', COUNT(*) FROM author_dim
                UNION ALL SELECT 'Categories', COUNT(*) FROM category_dim
                UNION ALL SELECT 'Publishers', COUNT(*) FROM publisher_dim
                UNION ALL SELECT 'Facts', COUNT(*) FROM fact_daily_product_metrics
            """)
            
            print("   📊 Data Warehouse Stats:")
            results = cursor.fetchall()
            for table_name, count in results:
                print(f"      • {table_name}: {count:,} records")
                
        except Exception as e:
            print(f"   ⚠️ Cannot show DW stats: {e}")
    
    def show_mart_stats(self, cursor):
        """Show DataMart statistics"""
        try:
            cursor.execute("""
                SELECT 'Daily Sales' as table_name, COUNT(*) as count FROM mart_daily_sales
                UNION ALL SELECT 'Category Performance', COUNT(*) FROM mart_category_performance
                UNION ALL SELECT 'Author Insights', COUNT(*) FROM mart_author_insights
                UNION ALL SELECT 'Publisher Performance', COUNT(*) FROM mart_publisher_performance
                UNION ALL SELECT 'Product Flat', COUNT(*) FROM mart_product_flat
            """)
            
            print("   📈 DataMart Stats:")
            results = cursor.fetchall()
            for table_name, count in results:
                print(f"      • {table_name}: {count:,} records")
                
        except Exception as e:
            print(f"   ⚠️ Cannot show DataMart stats: {e}")
    
    def run_quick_test(self):
        """Quick test của một vài stored procedures"""
        print("🧪 QUICK ETL TEST")
        print("=" * 40)
        
        # Test simple DW procedure
        dw_conn = self.connect_db('fahasa_dw')
        if dw_conn:
            cursor = dw_conn.cursor()
            try:
                print("   🔄 Testing sp_simple_load_publishers...")
                cursor.callproc('sp_simple_load_publishers')
                
                # Consume results
                for result in cursor.stored_results():
                    result.fetchall()
                    
                print("   ✅ DW test completed")
            except Exception as e:
                print(f"   ❌ DW test failed: {e}")
            finally:
                cursor.close()
                dw_conn.close()
        
        # Test simple DataMart procedure  
        mart_conn = self.connect_db('fahasa_datamart')
        if mart_conn:
            cursor = mart_conn.cursor()
            try:
                print("   🔄 Testing sp_populate_mart_daily_sales...")
                cursor.callproc('sp_populate_mart_daily_sales')
                
                # Consume results
                for result in cursor.stored_results():
                    result.fetchall()
                    
                print("   ✅ DataMart test completed")
            except Exception as e:
                print(f"   ❌ DataMart test failed: {e}")
            finally:
                cursor.close()
                mart_conn.close()
        
        print("🎉 Quick test completed!")


def main():
    """Main ETL runner"""
    
    # Check for arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg in ['--quick', '-q', 'quick']:
            print("🚀 QUICK MODE - Running full ETL transform...")
            etl = FahasaETL()
            success = etl.run_transform()
            sys.exit(0 if success else 1)
            
        elif arg in ['--test', '-t', 'test']:
            print("🧪 TEST MODE - Running quick test...")
            etl = FahasaETL()
            etl.run_quick_test()
            sys.exit(0)
            
        elif arg in ['--help', '-h', 'help']:
            print("FAHASA ETL TRANSFORM")
            print("Usage:")
            print("  python daily_etl.py --quick    # Run full transform")
            print("  python daily_etl.py --test     # Run quick test")
            print("  python daily_etl.py            # Interactive mode")
            sys.exit(0)
    
    # Interactive mode
    print("FAHASA ETL TRANSFORM PIPELINE")
    print("=" * 50)
    print("Chạy tất cả stored procedures:")
    print("  🏗️  DW Transform (8 procedures)")
    print("  📊 DataMart Transform (5 procedures)")  
    print("  🎯 BI Aggregates (5 procedures)")
    print()
    print("Opciones:")
    print("  1. Full transform (tất cả)")
    print("  2. Quick test (test một vài SP)")
    print("  3. Exit")
    print()
    
    choice = input("Chọn option (1-3): ").strip()
    
    if choice == '1':
        print("\n🚀 Running full ETL transform...")
        etl = FahasaETL()
        success = etl.run_transform()
        print(f"\n{'✅ Success!' if success else '❌ Failed!'}")
    elif choice == '2':
        print("\n🧪 Running quick test...")
        etl = FahasaETL()
        etl.run_quick_test()
    elif choice == '3':
        print("Exit")
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
from typing import Dict, List, Tuple, Optional
import argparse

# ======================================
# CONFIGURATION
# ======================================

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Will prompt if not provided
    'charset': 'utf8mb4'
}

CRAWLER_SCRIPT = 'daily_crawler.py'

# ======================================
# LOGGING SETUP
# ======================================

def setup_logging():
    """Setup logging for ETL process"""
    log_filename = f"etl_log_{date.today().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

# ======================================
# DATABASE OPERATIONS
# ======================================

class ETLProcessor:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        self.batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    def get_connection(self, database: str = None) -> mysql.connector.MySQLConnection:
        """Create database connection"""
        config = self.db_config.copy()
        if database:
            config['database'] = database
            
        if not config.get('password'):
            import getpass
            config['password'] = getpass.getpass("MySQL Password: ")
            
        return mysql.connector.connect(**config)
    
    def check_staging_data(self) -> int:
        """Check how many records are in staging"""
        try:
            conn = self.get_connection('fahasa_staging')
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM staging_books")
            count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            self.logger.info(f"Found {count} records in staging")
            return count
            
        except Exception as e:
            self.logger.error(f"Error checking staging data: {e}")
            return 0
    
    def run_simple_etl(self) -> Dict:
        """Run the ETL pipeline with simplified control logging"""
        try:
            conn = self.get_connection('fahasa_control')
            cursor = conn.cursor()
            
            self.logger.info(f"Starting ETL with simplified control logging - batch_id: {self.batch_id}")
            
            # Call the simplified daily ETL procedure
            cursor.callproc('sp_simplified_daily_etl', [])
            
            # Get results
            results = {}
            for result in cursor.stored_results():
                data = result.fetchall()
                
                if data and len(data) > 0:
                    row = data[0]
                    results = {
                        'log_id': row[0] if len(row) > 0 else None,
                        'staging_count': row[1] if len(row) > 1 else 0,
                        'authors_processed': row[2] if len(row) > 2 else 0,
                        'categories_processed': row[3] if len(row) > 3 else 0,
                        'products_processed': row[4] if len(row) > 4 else 0,
                        'facts_processed': row[5] if len(row) > 5 else 0,
                        'datamart_processed': row[6] if len(row) > 6 else 0,
                        'aggregates_processed': row[7] if len(row) > 7 else 0,
                        'total_duration': row[8] if len(row) > 8 else 0,
                        'status': row[9] if len(row) > 9 else 'COMPLETED'
                    }
            
            cursor.close()
            conn.close()
            
            self.logger.info("ETL completed successfully with simplified control logging")
            self.logger.info(f"Log ID: {results.get('log_id', 'N/A')}")
            self.logger.info(f"Total records processed: {results.get('staging_count', 0)}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"ETL failed: {e}")
            raise
    
    def generate_summary_report(self) -> Dict:
        """Generate summary report of data warehouse status"""
        try:
            conn = self.get_connection('fahasa_dw')
            cursor = conn.cursor()
            
            # Get dimension counts
            cursor.execute("SELECT COUNT(*) FROM author_dim")
            author_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM category_dim")
            category_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM product_dim")
            product_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM date_dim")
            date_count = cursor.fetchone()[0]
            
            # Check staging status
            conn_staging = self.get_connection('fahasa_staging')
            cursor_staging = conn_staging.cursor()
            
            cursor_staging.execute("SELECT COUNT(*) FROM staging_books")
            staging_count = cursor_staging.fetchone()[0]
            
            cursor_staging.execute("""
                SELECT MAX(time_collect) as latest_crawl 
                FROM staging_books 
                WHERE time_collect IS NOT NULL
            """)
            latest_crawl = cursor_staging.fetchone()[0]
            
            cursor_staging.close()
            conn_staging.close()
            
            cursor.close()
            conn.close()
            
            report = {
                'generated_at': datetime.now().isoformat(),
                'data_warehouse': {
                    'authors': author_count,
                    'categories': category_count,
                    'products': product_count,
                    'date_dimension': date_count
                },
                'staging': {
                    'total_records': staging_count,
                    'latest_crawl': latest_crawl.isoformat() if latest_crawl else None
                },
                'system_status': 'HEALTHY' if staging_count > 0 else 'NO_DATA'
            }
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating report: {e}")
            return {'error': str(e)}

# ======================================
# MAIN FUNCTIONS
# ======================================

def run_crawler():
    """Run the daily crawler script"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting daily crawler...")
        result = subprocess.run([sys.executable, CRAWLER_SCRIPT], 
                              capture_output=True, text=True, check=True)
        
        logger.info("Crawler completed successfully")
        logger.info(f"Crawler output: {result.stdout}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Crawler failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error(f"Crawler script not found: {CRAWLER_SCRIPT}")
        return False

def main():
    """Main ETL orchestration function"""
    parser = argparse.ArgumentParser(description='Daily ETL Pipeline')
    parser.add_argument('--with-crawler', action='store_true', 
                       help='Run crawler before ETL')
    parser.add_argument('--report', action='store_true',
                       help='Generate summary report only')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    logger.info("="*50)
    logger.info("DAILY ETL PIPELINE STARTED")
    logger.info("="*50)
    
    # Initialize ETL processor
    etl = ETLProcessor(DB_CONFIG)
    
    try:
        # Generate report only
        if args.report:
            logger.info("Generating summary report...")
            report = etl.generate_summary_report()
            
            print("\\n" + "="*60)
            print("DATA WAREHOUSE SUMMARY REPORT")
            print("="*60)
            print(json.dumps(report, indent=2, default=str))
            print("="*60)
            
            return
        
        # Run crawler if requested
        if args.with_crawler:
            if not run_crawler():
                logger.error("Crawler failed, aborting ETL")
                return
        
        # Check staging data
        staging_count = etl.check_staging_data()
        if staging_count == 0:
            logger.warning("No staging data found - consider running crawler first")
            logger.info("Use --with-crawler flag to run crawler automatically")
            return
        
        # Run ETL
        logger.info("Starting ETL transformation...")
        results = etl.run_simple_etl()
        
        # Print summary
        print("\\n" + "="*60)
        print("ETL EXECUTION SUMMARY")
        print("="*60)
        print(f"Batch ID: {etl.batch_id}")
        print(f"Log ID: {results.get('log_id', 'N/A')}")
        print(f"Staging Records: {results.get('staging_count', 0)}")
        print(f"Authors Processed: {results.get('authors_processed', 0)}")
        print(f"Categories Processed: {results.get('categories_processed', 0)}")
        print(f"Products Processed: {results.get('products_processed', 0)}")
        print(f"Facts Processed: {results.get('facts_processed', 0)}")
        print(f"DataMart Records: {results.get('datamart_processed', 0)}")
        print(f"BI Aggregates: {results.get('aggregates_processed', 0)}")
        print(f"Duration: {results.get('total_duration', 0)} seconds")
        print(f"Status: {results.get('status', 'COMPLETED')}")
        print("="*60)
        
        logger.info("ETL pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()