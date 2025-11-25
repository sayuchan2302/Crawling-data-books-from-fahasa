#!/usr/bin/env python3
"""
Business Intelligence Aggregator
Handles all aggregate table processing for Data Warehouse
"""
import mysql.connector
import sys
import os
from datetime import datetime

# Add utils path for control logging
utils_path = os.path.join(os.path.dirname(__file__), '..', '..', 'utils')
sys.path.append(utils_path)

try:
    from control_logger import ControlLogger
    logger_available = True
except ImportError:
    print("Warning: Control logger not available")
    logger_available = False

class BIAggregator:
    """
    Business Intelligence Aggregator
    Handles all aggregate table processing for Data Warehouse
    """
    
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'fahasa_dw',
            'charset': 'utf8mb4'
        }
        self.connection = None
        self.logger = ControlLogger() if logger_available else None
        
    def connect(self):
        """Connect to DW database"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def run_all_aggregates(self):
        """
        Run all BI aggregate procedures
        Returns: (success_count, total_count, results)
        """
        aggregate_log_id = None
        
        try:
            print("STARTING BI AGGREGATES PROCESSING")
            print("=" * 50)
            
            # Log aggregate start
            if self.logger:
                aggregate_log_id = self.logger.log_etl_start(
                    "BI_AGGREGATES",
                    "fahasa_dw.all_tables", 
                    "BI aggregate tables"
                )
            
            # Connect to database
            if not self.connect():
                raise Exception("Failed to connect to database")
            
            # Define aggregate procedures
            procedures = [
                {
                    'name': 'sp_populate_author_performance',
                    'description': 'Author Performance Aggregate',
                    'table': 'author_performance_aggregate'
                },
                {
                    'name': 'sp_populate_publisher_revenue', 
                    'description': 'Publisher Revenue Aggregate',
                    'table': 'publisher_revenue_aggregate'
                },
                {
                    'name': 'sp_populate_category_sales',
                    'description': 'Category Sales Aggregate', 
                    'table': 'category_sales_aggregate'
                },
                {
                    'name': 'sp_populate_price_range',
                    'description': 'Price Range Aggregate',
                    'table': 'price_range_aggregate'
                },
                {
                    'name': 'sp_populate_rating_distribution',
                    'description': 'Rating Distribution Aggregate',
                    'table': 'rating_aggregate'
                }
            ]
            
            # Execute each procedure
            success_count = 0
            results = []
            
            cursor = self.connection.cursor()
            
            for proc in procedures:
                try:
                    print(f"   Processing {proc['description']}...")
                    
                    # Execute stored procedure
                    cursor.callproc(proc['name'])
                    
                    # Get result count if possible
                    try:
                        for result in cursor.stored_results():
                            rows = result.fetchall()
                            if rows:
                                print(f"   {proc['description']}: {rows[0][0]} records processed")
                    except:
                        # If no result set, just check table count
                        cursor.execute(f"SELECT COUNT(*) FROM {proc['table']}")
                        count = cursor.fetchone()[0]
                        print(f"   {proc['description']}: {count:,} total records")
                    
                    success_count += 1
                    results.append({
                        'procedure': proc['name'],
                        'description': proc['description'],
                        'status': 'SUCCESS'
                    })
                    
                    print(f"   {proc['description']} completed")
                    
                except Exception as e:
                    results.append({
                        'procedure': proc['name'],
                        'description': proc['description'], 
                        'status': 'FAILED',
                        'error': str(e)
                    })
                    print(f"   {proc['description']}: Failed - {e}")
            
            # Commit all changes
            self.connection.commit()
            cursor.close()
            
            # Log success
            if self.logger and aggregate_log_id:
                summary = f"BI Aggregates completed: {success_count}/{len(procedures)} successful"
                self.logger.log_etl_success(aggregate_log_id, summary)
            
            print(f"\nBI AGGREGATES COMPLETED: {success_count}/{len(procedures)} successful")
            
            return success_count, len(procedures), results
            
        except Exception as e:
            print(f"\nBI Aggregates failed: {e}")
            if self.logger and aggregate_log_id:
                self.logger.log_etl_error(aggregate_log_id, str(e))
            return 0, len(procedures) if 'procedures' in locals() else 0, []
        finally:
            self.disconnect()
    
    def run_specific_aggregate(self, procedure_name):
        """
        Run a specific aggregate procedure
        Args: procedure_name (str): Name of procedure to run
        """
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
                
            cursor = self.connection.cursor()
            print(f"Running {procedure_name}...")
            
            cursor.callproc(procedure_name)
            self.connection.commit()
            
            print(f" {procedure_name} completed")
            cursor.close()
            
        except Exception as e:
            print(f" {procedure_name} failed: {e}")
            raise
        finally:
            self.disconnect()
    
    def show_aggregate_stats(self):
        """Show statistics for all aggregate tables"""
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
                
            cursor = self.connection.cursor()
            
            tables = [
                'author_performance_aggregate',
                'publisher_revenue_aggregate', 
                'category_sales_aggregate',
                'price_range_aggregate',
                'rating_aggregate'
            ]
            
            print("\n BI AGGREGATE STATISTICS:")
            print("-" * 40)
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"   {table}: {count:,} records")
                except Exception as e:
                    print(f"   {table}: Error - {e}")
            
            cursor.close()
            
        except Exception as e:
            print(f"Cannot show aggregate stats: {e}")
        finally:
            self.disconnect()

    def test_connection(self):
        """Test database connection"""
        try:
            if self.connect():
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                print("Database connection successful")
                return True
            else:
                print("Database connection failed")
                return False
        except Exception as e:
            print(f"Database connection test failed: {e}")
            return False
        finally:
            self.disconnect()


def main():
    """Main function for standalone execution"""
    print(" FAHASA BI AGGREGATOR")
    print("Processing Data Warehouse aggregates...")
    print()
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == '--stats' or command == '-s':
            # Show statistics only
            aggregator = BIAggregator()
            aggregator.show_aggregate_stats()
            return
        elif command == '--test' or command == '-t':
            # Test connection
            aggregator = BIAggregator()
            aggregator.test_connection()
            return
        elif command == '--help' or command == '-h':
            print("Usage:")
            print("  python bi_aggregator.py          # Run all aggregates")
            print("  python bi_aggregator.py --stats  # Show statistics")
            print("  python bi_aggregator.py --test   # Test connection")
            print("  python bi_aggregator.py --help   # Show this help")
            return
    
    # Run all aggregates
    aggregator = BIAggregator()
    success_count, total_count, results = aggregator.run_all_aggregates()
    
    # Show final summary
    print(f"\n FINAL SUMMARY:")
    print(f"   Successful: {success_count}/{total_count}")
    print(f"   Failed: {total_count - success_count}/{total_count}")
    
    # Show aggregate stats
    aggregator.show_aggregate_stats()


if __name__ == "__main__":
    main()