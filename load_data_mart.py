#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load Data Mart Script
Populate Fahasa DataMart from Data Warehouse using SQL Stored Procedures
Handles all 5 mart tables with optimal performance and SQL-based transformations
"""

import mysql.connector
import traceback
from datetime import datetime
import sys
import os

# Add utils path for logging
utils_path = os.path.join(os.path.dirname(__file__), 'utils')
sys.path.append(utils_path)

try:
    from control_logger import ControlLogger
    logger_available = True
except ImportError:
    print("⚠️ Control logger not available")
    logger_available = False

class DataMartLoader:
    """Load data from fahasa_dw to fahasa_datamart using stored procedures"""
    
    def __init__(self):
        self.mart_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'fahasa_datamart',
            'charset': 'utf8mb4'
        }
        self.mart_conn = None
        self.logger = ControlLogger() if logger_available else None
        
    def connect_database(self):
        """Connect to DataMart database (SP will handle cross-DB queries)"""
        try:
            # Connect to DataMart (stored procedures handle DW access)
            self.mart_conn = mysql.connector.connect(**self.mart_config)
            print("✅ Connected to fahasa_datamart")
            
            return True
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect_database(self):
        """Close database connections"""
        if self.mart_conn:
            self.mart_conn.close()
    
    def call_stored_procedure(self, sp_name, description):
        """Execute a stored procedure and return status"""
        print(f"\n📊 {description}...")
        
        try:
            cursor = self.mart_conn.cursor()
            
            # Call the stored procedure
            print(f"   🔄 Executing {sp_name}...")
            cursor.callproc(sp_name)
            
            # Commit the transaction
            self.mart_conn.commit()
            
            # Get procedure results (if any)
            for result in cursor.stored_results():
                result_data = result.fetchall()
                if result_data:
                    print(f"   📋 Procedure output: {result_data}")
            
            cursor.close()
            print(f"   ✅ {sp_name} completed successfully")
            return True
            
        except Exception as e:
            print(f"   ❌ Error executing {sp_name}: {e}")
            if "doesn't exist" in str(e):
                print(f"      💡 Make sure the stored procedure {sp_name} is created in fahasa_datamart")
            traceback.print_exc()
            return False
    
    def load_mart_author_insights(self):
        """Load mart_author_insights using stored procedure"""
        return self.call_stored_procedure('sp_load_mart_author_insights', 'Loading Author Insights')
    
    def load_mart_category_performance(self):
        """Load mart_category_performance using stored procedure"""
        return self.call_stored_procedure('sp_load_mart_category_performance', 'Loading Category Performance')
    
    def load_mart_daily_sales(self):
        """Load mart_daily_sales using Python approach (due to SP limitations)"""
        print(f"📊 Loading Daily Sales...")
        print(f"   🔄 Executing Python-based daily sales loader...")
        
        try:
            cursor = self.mart_conn.cursor()
            
            # Clear table
            cursor.execute("TRUNCATE TABLE mart_daily_sales")
            
            # Get data from DW
            query = """
            SELECT 
                dd.full_date as date,
                fbs.book_sk as product_id,
                COALESCE(MAX(pd.product_name), MAX(db.title), 'Unknown Product') as product_name,
                COALESCE(GROUP_CONCAT(DISTINCT dc.category_path SEPARATOR ', '), 'Uncategorized') as category_path,
                COALESCE(MAX(db.publisher), 'Unknown') as publisher_name,
                COALESCE(MAX(db.author), 'Unknown') as author_names,
                COALESCE(MAX(fbs.original_price), 0) as price,
                COALESCE(MAX(fbs.discount_price), MAX(fbs.original_price), 0) as discount_price,
                COALESCE(MAX(fbs.discount_percent), 0) as discount_percent,
                COALESCE(MAX(fbs.rating), 0) as rating,
                COALESCE(MAX(fbs.rating_count), 0) as rating_count,
                COALESCE(MAX(fbs.sold_count_numeric), 0) as sold_today,
                COALESCE(MAX(fbs.sold_count_numeric), 0) as sold_cumulative
            FROM fahasa_dw.fact_book_sales fbs
            JOIN fahasa_dw.dim_date dd ON fbs.date_sk = dd.date_sk
            LEFT JOIN fahasa_dw.product_dim pd ON fbs.book_sk = pd.id
            LEFT JOIN fahasa_dw.dim_books db ON fbs.book_sk = db.book_sk
            LEFT JOIN fahasa_dw.bridge_book_categories bbc ON fbs.book_sk = bbc.book_sk
            LEFT JOIN fahasa_dw.dim_categories dc ON bbc.category_sk = dc.category_sk
            WHERE dd.full_date IS NOT NULL
            GROUP BY dd.full_date, fbs.book_sk;
            """
            
            cursor.execute(query)
            data = cursor.fetchall()
            
            if len(data) > 0:
                # Insert data
                insert_query = """
                INSERT INTO mart_daily_sales (
                    date, product_id, product_name, category_path, publisher_name,
                    author_names, price, discount_price, discount_percent, rating,
                    rating_count, sold_today, sold_cumulative
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, data)
                self.mart_conn.commit()
                
                print(f"   ✅ Daily Sales completed successfully")
                print(f"   ✅ Daily Sales: SUCCESS")
                cursor.close()
                return True
            else:
                print(f"   ⚠️ No data found in DW for daily sales")
                print(f"   ✅ Daily Sales: SUCCESS (no data)")
                cursor.close()
                return True
                
        except Exception as e:
            print(f"   ❌ Daily Sales failed: {e}")
            return False
    
    def load_mart_product_flat(self):
        """Load mart_product_flat using Python approach (due to SP aggregation issues)"""
        print(f"📊 Loading Product Flat...")
        print(f"   🔄 Executing Python-based product flat loader...")
        
        try:
            cursor = self.mart_conn.cursor()
            
            # Clear table
            cursor.execute("TRUNCATE TABLE mart_product_flat")
            
            # Get data from DW with correct ID mapping
            query = """
            SELECT 
                pd.id as product_id,
                pd.product_name,
                COALESCE(pd.isbn, 'N/A') as isbn,
                COALESCE(GROUP_CONCAT(DISTINCT dc.category_path SEPARATOR ', '), 'Uncategorized') as category_path,
                COALESCE(MAX(db.author), 'Unknown') as author_names,
                COALESCE(MAX(db.publisher), 'Unknown') as publisher_name,
                COALESCE(pd.page_count, 0) as page_count,
                COALESCE(pd.weight, 0) as weight,
                COALESCE(pd.dimensions, 'N/A') as dimensions,
                COALESCE(pd.language, 'N/A') as language,
                COALESCE(MIN(dd.full_date), CURDATE()) as first_seen,
                COALESCE(MAX(dd.full_date), CURDATE()) as last_seen,
                COALESCE(SUM(fbs.sold_count_numeric), 0) as total_sold,
                COALESCE(ROUND(AVG(CASE WHEN fbs.rating > 0 THEN fbs.rating END), 2), 0) as avg_rating,
                COALESCE(ROUND(AVG(CASE WHEN fbs.original_price > 0 THEN fbs.original_price END), 2), 0) as avg_price
            FROM fahasa_dw.product_dim pd
            LEFT JOIN fahasa_dw.dim_books db ON (pd.id + 25) = db.book_sk
            LEFT JOIN fahasa_dw.bridge_book_categories bbc ON (pd.id + 25) = bbc.book_sk
            LEFT JOIN fahasa_dw.dim_categories dc ON bbc.category_sk = dc.category_sk
            LEFT JOIN fahasa_dw.fact_book_sales fbs ON (pd.id + 25) = fbs.book_sk
            LEFT JOIN fahasa_dw.dim_date dd ON fbs.date_sk = dd.date_sk
            GROUP BY pd.id, pd.product_name, pd.isbn, pd.page_count, pd.weight, 
                     pd.dimensions, pd.language
            ORDER BY total_sold DESC;
            """
            
            cursor.execute(query)
            data = cursor.fetchall()
            
            if len(data) > 0:
                # Insert data
                insert_query = """
                INSERT INTO mart_product_flat (
                    product_id, product_name, isbn, category_path, author_names,
                    publisher_name, page_count, weight, dimensions, language,
                    first_seen, last_seen, total_sold, avg_rating, avg_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, data)
                self.mart_conn.commit()
                
                print(f"   ✅ Product Flat completed successfully")
                print(f"   ✅ Product Flat: SUCCESS")
                cursor.close()
                return True
            else:
                print(f"   ⚠️ No data found in DW for product flat")
                print(f"   ✅ Product Flat: SUCCESS (no data)")
                cursor.close()
                return True
                
        except Exception as e:
            print(f"   ❌ Product Flat failed: {e}")
            return False
    
    def load_mart_publisher_performance(self):
        """Load mart_publisher_performance using stored procedure"""
        return self.call_stored_procedure('sp_load_mart_publisher_performance', 'Loading Publisher Performance')
    
    def verify_datamart(self):
        """Verify loaded data in datamart"""
        print("\n🔍 VERIFYING DATAMART DATA...")
        
        try:
            cursor = self.mart_conn.cursor()
            
            tables = [
                'mart_author_insights',
                'mart_category_performance', 
                'mart_daily_sales',
                'mart_product_flat',
                'mart_publisher_performance'
            ]
            
            total_records = 0
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_records += count
                
                print(f"   📊 {table}: {count:,} records")
                
                # Sample data check
                if count > 0:
                    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                    sample = cursor.fetchone()
                    print(f"      ✅ Sample record exists")
                else:
                    print(f"      ⚠️ Table is empty")
            
            print(f"\n📊 TOTAL DATAMART RECORDS: {total_records:,}")
            
            cursor.close()
            return total_records > 0
            
        except Exception as e:
            print(f"❌ Verification error: {e}")
            return False
    
    def check_stored_procedures(self):
        """Check if all required stored procedures exist"""
        print("\n🔍 CHECKING STORED PROCEDURES...")
        
        try:
            cursor = self.mart_conn.cursor()
            
            required_sps = [
                'sp_load_mart_author_insights',
                'sp_load_mart_category_performance', 
                # 'sp_load_mart_daily_sales', # Using Python approach
                # 'sp_load_mart_product_flat', # Using Python approach  
                'sp_load_mart_publisher_performance'
            ]
            
            missing_sps = []
            
            for sp in required_sps:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.ROUTINES 
                    WHERE ROUTINE_SCHEMA = 'fahasa_datamart' 
                    AND ROUTINE_NAME = %s 
                    AND ROUTINE_TYPE = 'PROCEDURE'
                """, (sp,))
                
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    print(f"   ✅ {sp}")
                else:
                    print(f"   ❌ {sp} - MISSING")
                    missing_sps.append(sp)
            
            cursor.close()
            
            if missing_sps:
                print(f"\n⚠️ MISSING STORED PROCEDURES: {len(missing_sps)}")
                print("   💡 Please run fahasa_datamart_stored_procedures.sql first")
                return False
            else:
                print(f"\n✅ All stored procedures are available")
                return True
                
        except Exception as e:
            print(f"❌ Error checking stored procedures: {e}")
            return False
    
    def run_full_load(self):
        """Run complete datamart load using stored procedures"""
        load_id = None
        
        try:
            print("🎯 FAHASA DATAMART LOAD PROCESS")
            print("=" * 50)
            print(f"⏰ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Log load start
            load_id = None
            if self.logger:
                try:
                    load_id = self.logger.log_etl_start(
                        process_name="DATAMART_LOAD",
                        source_table="fahasa_dw.*",
                        target_table="fahasa_datamart.*"
                    )
                except AttributeError:
                    print("⚠️ Logger method not available, continuing without logging")
            
            # Connect to database
            if not self.connect_database():
                raise Exception("Database connection failed")
            
            # Check stored procedures
            if not self.check_stored_procedures():
                raise Exception("Required stored procedures are missing")
            
            # Load all mart tables using stored procedures
            success_count = 0
            total_count = 5
            
            loaders = [
                ("Author Insights", self.load_mart_author_insights),
                ("Category Performance", self.load_mart_category_performance),
                ("Daily Sales", self.load_mart_daily_sales),
                ("Product Flat", self.load_mart_product_flat),
                ("Publisher Performance", self.load_mart_publisher_performance)
            ]
            
            for name, loader_func in loaders:
                try:
                    if loader_func():
                        success_count += 1
                        print(f"   ✅ {name}: SUCCESS")
                    else:
                        print(f"   ❌ {name}: FAILED")
                except Exception as e:
                    print(f"   ❌ {name}: ERROR - {e}")
            
            # Verify datamart
            verification_ok = self.verify_datamart()
            
            # Log completion
            if self.logger and load_id:
                try:
                    if success_count == total_count and verification_ok:
                        self.logger.log_etl_success(
                            etl_run_id=load_id,
                            records_processed=f"{success_count}/{total_count} marts loaded",
                            target_table="fahasa_datamart.*"
                        )
                    else:
                        self.logger.log_etl_error(
                            etl_run_id=load_id,
                            error_message=f"Incomplete load: {success_count}/{total_count}",
                            target_table="fahasa_datamart.*"
                        )
                except AttributeError:
                    print("⚠️ Logger completion method not available")
            
            print(f"\n📊 FINAL RESULTS:")
            print(f"   ✅ Success: {success_count}/{total_count} tables")
            print(f"   ⚠️ Failed: {total_count - success_count}/{total_count} tables")
            print(f"   🔍 Verification: {'PASSED' if verification_ok else 'FAILED'}")
            
            if success_count == total_count and verification_ok:
                print(f"\n🎉 DATAMART LOAD COMPLETED SUCCESSFULLY!")
                print(f"🚀 Ready for BI reporting and analytics!")
                return True
            else:
                print(f"\n⚠️ DATAMART LOAD PARTIALLY COMPLETED!")
                return False
                
        except Exception as e:
            print(f"\n❌ DATAMART LOAD FAILED: {e}")
            if self.logger and load_id:
                try:
                    self.logger.log_etl_error(
                        etl_run_id=load_id,
                        error_message=str(e),
                        target_table="fahasa_datamart.*"
                    )
                except AttributeError:
                    print("⚠️ Logger error method not available")
            return False
        finally:
            self.disconnect_database()

def main():
    """Main function for standalone execution"""
    print("🎯 FAHASA DATAMART LOADER")
    print("Loading data from DW to DataMart using SQL Stored Procedures...")
    print()
    
    loader = DataMartLoader()
    success = loader.run_full_load()
    
    if success:
        print(f"\n✅ DataMart load completed successfully!")
    else:
        print(f"\n❌ DataMart load failed or incomplete!")
        print(f"💡 Make sure fahasa_datamart_stored_procedures.sql has been executed")
    
    return success

if __name__ == "__main__":
    main()