#!/usr/bin/env python3
"""
Daily ETL Transform Pipeline - Staging to Data Warehouse
Scope: Transform staging data into Data Warehouse (DW)
Note: Use load_data_mart.py separately for DataMart population
"""

import mysql.connector
from mysql.connector import Error
import sys
import os
import subprocess
import logging
import argparse
import json
from datetime import datetime, date

# Add path for control logging
sys.path.append(os.path.dirname(__file__))
try:
    from control_logger import ETLLogger
    control_logging_available = True
except ImportError:
    print("⚠️ Control logger not available - running without logging")
    control_logging_available = False
    
    class MockETLLogger:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def update_progress(self, *args, **kwargs):
            pass
    
    ETLLogger = MockETLLogger

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
        """Run ETL transform pipeline: Staging → Data Warehouse"""
        
        # Use control logging context manager
        etl_details = {
            'pipeline_type': 'staging_to_dw_transform',
            'components': ['staging_to_dw', 'bi_aggregates'],
            'mode': 'transform_only'
        }
        
        with ETLLogger('fahasa_staging_to_dw_transform', etl_details) as etl_log:
            try:
                print("🚀 FAHASA STAGING TO DW TRANSFORM PIPELINE")
                print("=" * 60)
                print(f"📅 Start Time: {datetime.now()}")
                print(f"🎯 Scope: Transform Staging → Data Warehouse Only")
                print("=" * 60)
                
                # Step 1: Transform Staging → DW
                print("📊 STEP 1: STAGING → DATA WAREHOUSE TRANSFORM")
                print("-" * 50)
                dw_success = self.run_dw_transform()
                
                if not dw_success:
                    raise Exception("DW Transform failed")
                
                etl_log.update_progress(processed=1, inserted=1)  # Track step completion
                
                # Step 2: Generate BI Aggregates (DW level)
                print("\n🎯 STEP 2: GENERATE DW BI AGGREGATES")
                print("-" * 50)
                bi_success = self.run_bi_aggregates()
                
                if not bi_success:
                    print("⚠️ BI Aggregates failed but continuing...")
                
                etl_log.update_progress(processed=2, inserted=2)  # All steps completed
                
                print("\n🎉 STAGING → DW TRANSFORM COMPLETED!")
                print("✅ Data Warehouse is ready for DataMart loading")
                print(f"💡 Note: Use load_data_mart.py for DataMart population")
                print(f"📅 End Time: {datetime.now()}")
                
                return True
                
            except Exception as e:
                print(f"\n❌ ETL Transform failed: {e}")
                raise  # Let context manager handle the error
    
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
    

    
    def run_quick_test(self):
        """Quick test of DW transform procedures"""
        print("🧪 QUICK DW TRANSFORM TEST")
        print("=" * 40)
        print("🎯 Testing: Staging → Data Warehouse Transform")
        print()
        
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
                    
                print("   ✅ DW transform test completed")
                
                # Show simple DW stats
                cursor.execute("SELECT COUNT(*) FROM publisher_dim")
                pub_count = cursor.fetchone()[0]
                print(f"   📊 Publishers in DW: {pub_count}")
                
            except Exception as e:
                print(f"   ❌ DW transform test failed: {e}")
            finally:
                cursor.close()
                dw_conn.close()
        
        print("\n💡 Note: For DataMart testing, use load_data_mart.py")
        print("🎉 DW transform quick test completed!")

# ======================================
# MODERN ETL FUNCTIONS
# ======================================
from typing import Dict, List, Tuple, Optional
import argparse
import getpass

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
        """Run the DW ETL pipeline (staging→dw) with simplified control logging"""
        try:
            conn = self.get_connection('fahasa_control')
            cursor = conn.cursor()
            
            self.logger.info(f"Starting DW ETL (staging→dw) with simplified control logging - batch_id: {self.batch_id}")
            
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
    
    def run_bi_aggregates_only(self) -> Dict:
        """Run only BI aggregates using modular BIAggregator"""
        try:
            from src.etl.bi_aggregator import BIAggregator
            
            self.logger.info("Starting BI aggregates processing...")
            
            # Initialize BI Aggregator
            bi_aggregator = BIAggregator()
            
            # Run all aggregates
            success_count, total_count, results = bi_aggregator.run_all_aggregates()
            
            # Create summary
            summary = {
                'batch_id': self.batch_id,
                'bi_aggregates_success': success_count,
                'bi_aggregates_total': total_count,
                'bi_success_rate': (success_count / total_count * 100) if total_count > 0 else 0,
                'status': 'COMPLETED' if success_count == total_count else 'PARTIAL',
                'details': results
            }
            
            self.logger.info(f"BI Aggregates completed: {success_count}/{total_count} successful")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"BI Aggregates failed: {e}")
            raise
    
    def run_etl_with_modular_bi(self) -> Dict:
        """Run DW ETL pipeline with modular BI aggregates"""
        try:
            # Run main DW ETL first
            self.logger.info("Starting modular DW ETL pipeline...")
            
            # Run core DW ETL (staging→dw)
            etl_results = self.run_simple_etl()
            
            # Run DW BI aggregates separately using modular approach
            bi_results = self.run_bi_aggregates_only()
            
            # Combine results
            combined_results = {
                **etl_results,
                'bi_modular_success': bi_results['bi_aggregates_success'],
                'bi_modular_total': bi_results['bi_aggregates_total'],
                'bi_modular_rate': bi_results['bi_success_rate'],
                'modular_status': bi_results['status'],
                'architecture': 'MODULAR_DW_ONLY',
                'note': 'Use load_data_mart.py for DataMart population'
            }
            
            self.logger.info("Modular DW ETL pipeline completed successfully")
            self.logger.info("Note: Use load_data_mart.py for DataMart population")
            
            return combined_results
            
        except Exception as e:
            self.logger.error(f"Modular DW ETL pipeline failed: {e}")
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

def show_interactive_menu():
    """Show interactive menu for ETL options"""
    print("FAHASA STAGING → DW TRANSFORM PIPELINE")
    print("=" * 55)
    print("🎯 Scope: Transform Staging data to Data Warehouse")
    print("💡 Note: Use load_data_mart.py separately for DataMart")
    print()
    print("Choose transform approach:")
    print("  🏗️  Traditional: FahasaETL class (staging→dw transform)")
    print("  🚀 Modern: ETLProcessor class (enhanced control)")
    print()
    print("Options:")
    print("  1. Traditional DW Transform (FahasaETL)")
    print("  2. Traditional Quick Test (DW only)")
    print("  3. Modern ETL (ETLProcessor)")
    print("  4. Modern ETL + BI Aggregates")
    print("  5. BI Aggregates Only (DW level)")
    print("  6. Show Help")
    print("  7. Exit")
    print()
    
    choice = input("Choose option (1-7): ").strip()
    
    if choice == '1':
        print("\n🚀 Running traditional DW transform (Staging→DW)...")
        etl = FahasaETL()
        success = etl.run_transform()
        print(f"\n{'✅ Success!' if success else '❌ Failed!'}")
        if success:
            print("💡 Next: Run load_data_mart.py for DataMart population")
    elif choice == '2':
        print("\n🧪 Running traditional DW quick test...")
        etl = FahasaETL()
        etl.run_quick_test()
    elif choice == '3':
        print("\n🔥 Running modern DW ETL (ETLProcessor)...")
        etl = ETLProcessor(DB_CONFIG)
        results = etl.run_simple_etl()
        print(f"\n✅ Modern DW ETL completed! Status: {results.get('status', 'UNKNOWN')}")
        print("💡 Next: Run load_data_mart.py for DataMart population")
    elif choice == '4':
        print("\n⚡ Running modern DW ETL + BI aggregates...")
        etl = ETLProcessor(DB_CONFIG)
        results = etl.run_etl_with_modular_bi()
        print(f"\n🎉 DW ETL + BI completed! Architecture: {results.get('architecture', 'MODERN')}")
        print("💡 Next: Run load_data_mart.py for DataMart population")
    elif choice == '5':
        print("\n🎯 Running DW BI aggregates only...")
        etl = ETLProcessor(DB_CONFIG)
        results = etl.run_bi_aggregates_only()
        print(f"\n📊 DW BI Aggregates completed! Success rate: {results.get('bi_success_rate', 0):.1f}%")
    elif choice == '6':
        print("\nFAHASA STAGING→DW TRANSFORM HELP:")
        print("🎯 Scope: Transform staging data to data warehouse")
        print("\nCommand line options:")
        print("  (none)          Auto mode: ETL + BI aggregates")
        print("  --quick, -q     Traditional DW transform (staging→dw)")
        print("  --test, -t      Traditional DW quick test")
        print("  --menu, -m      Show this interactive menu")
        print("  --modular       Modern DW ETL with BI aggregates")
        print("  --bi-only       DW BI aggregates only")
        print("  --report        DW summary report")
        print("  --with-crawler  Run crawler before transform")
        print("\n📋 Workflow:")
        print("  1. daily_etl.py     → Transform staging to DW")
        print("  2. load_data_mart.py → Load DW to DataMart")
        print("\n💡 For DataMart: Use load_data_mart.py separately")
    elif choice == '7':
        print("👋 Exit")
    else:
        print("❌ Invalid choice")

def main():
    """Main ETL orchestration function - Unified approach"""
    parser = argparse.ArgumentParser(description='Fahasa Daily ETL Pipeline - Unified')
    
    # Legacy options (FahasaETL class)
    parser.add_argument('--quick', '-q', action='store_true',
                       help='Run full transform using traditional FahasaETL class')
    parser.add_argument('--test', '-t', action='store_true', 
                       help='Run quick test using traditional approach')
    
    # Modern options (ETLProcessor class)
    parser.add_argument('--with-crawler', action='store_true', 
                       help='Run crawler before ETL')
    parser.add_argument('--report', action='store_true',
                       help='Generate summary report only')
    parser.add_argument('--bi-only', action='store_true',
                       help='Run BI aggregates only using modular approach')
    parser.add_argument('--modular', action='store_true',
                       help='Run ETL with modular BI aggregates')
    parser.add_argument('--menu', '-m', action='store_true',
                       help='Show interactive menu (original behavior)')
    
    args = parser.parse_args()
    
    # Handle legacy options first
    if args.quick:
        print("🚀 QUICK MODE - Running full ETL transform (Traditional)...")
        etl = FahasaETL()
        success = etl.run_transform()
        print(f"\n{'✅ Success!' if success else '❌ Failed!'}")
        sys.exit(0 if success else 1)
        
    if args.test:
        print("🧪 TEST MODE - Running quick test (Traditional)...")
        etl = FahasaETL()
        etl.run_quick_test()
        sys.exit(0)
    
    # Show interactive menu if requested
    if args.menu:
        show_interactive_menu()
        return
    
    # If no arguments, run default ETL with BI aggregates
    if not any(vars(args).values()):
        print("🚀 FAHASA STAGING → DW TRANSFORM PIPELINE")
        print("=" * 60)
        print("🎯 Auto Mode: Running ETL Transform + BI Aggregates")
        print("💡 Note: Use load_data_mart.py separately for DataMart")
        print("=" * 60)
        
        # Run default: Traditional ETL transform + BI aggregates
        etl = FahasaETL()
        success = etl.run_transform()
        
        if success:
            print(f"\n✅ ETL + BI AGGREGATES COMPLETED SUCCESSFULLY!")
            print("🎯 Data Warehouse is ready with transformed data and aggregates")
            print("💡 Next: Run load_data_mart.py for DataMart population")
        else:
            print(f"\n❌ ETL TRANSFORM FAILED!")
        
        sys.exit(0 if success else 1)
    
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
        
        # Run BI aggregates only
        if args.bi_only:
            logger.info("Running BI aggregates only (modular approach)...")
            results = etl.run_bi_aggregates_only()
            
            print("\\n" + "="*60)
            print("BI AGGREGATES EXECUTION SUMMARY")
            print("="*60)
            print(f"Batch ID: {etl.batch_id}")
            print(f"BI Aggregates Success: {results.get('bi_aggregates_success', 0)}")
            print(f"BI Aggregates Total: {results.get('bi_aggregates_total', 0)}")
            print(f"Success Rate: {results.get('bi_success_rate', 0):.1f}%")
            print(f"Status: {results.get('status', 'UNKNOWN')}")
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
        
        # Run ETL - choose modular or traditional approach
        if args.modular:
            logger.info("Starting modular ETL transformation...")
            results = etl.run_etl_with_modular_bi()
        else:
            logger.info("Starting traditional ETL transformation...")
            results = etl.run_simple_etl()
        
        # Print summary
        print("\\n" + "="*60)
        print("STAGING → DW TRANSFORM EXECUTION SUMMARY")
        print("="*60)
        print(f"Batch ID: {etl.batch_id}")
        print(f"Log ID: {results.get('log_id', 'N/A')}")
        print(f"Staging Records: {results.get('staging_count', 0)}")
        print(f"Authors Processed: {results.get('authors_processed', 0)}")
        print(f"Categories Processed: {results.get('categories_processed', 0)}")
        print(f"Products Processed: {results.get('products_processed', 0)}")
        print(f"Facts Processed: {results.get('facts_processed', 0)}")
        
        # Show BI aggregates info based on approach
        if args.modular:
            print(f"DW BI Modular Success: {results.get('bi_modular_success', 0)}")
            print(f"DW BI Modular Total: {results.get('bi_modular_total', 0)}")
            print(f"DW BI Success Rate: {results.get('bi_modular_rate', 0):.1f}%")
            print(f"Architecture: {results.get('architecture', 'TRADITIONAL')}")
        else:
            print(f"DW BI Aggregates: {results.get('aggregates_processed', 0)}")
        
        print(f"Duration: {results.get('total_duration', 0)} seconds")
        print(f"Status: {results.get('status', 'COMPLETED')}")
        print("\n💡 Next Step: Run load_data_mart.py for DataMart population")
        print("="*60)
        
        logger.info("ETL pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()