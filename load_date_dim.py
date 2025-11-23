#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load Date Dimension Script
Load date_dim.csv into fahasa_dw.date_dim table
"""

import mysql.connector
import csv
import os
import sys
from datetime import datetime
import traceback

class DateDimLoader:
    """Load date dimension from CSV file"""
    
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'fahasa_dw',
            'charset': 'utf8mb4',
            'autocommit': False  # Use transactions for safety
        }
        self.csv_file = os.path.join(os.path.dirname(__file__), 'date_dim', 'date_dim.csv')
        self.conn = None
        
    def connect_database(self):
        """Connect to database"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            print("✅ Connected to fahasa_dw database")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect_database(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed")
    
    def check_csv_file(self):
        """Check if CSV file exists and get basic info"""
        try:
            if not os.path.exists(self.csv_file):
                print(f"❌ CSV file not found: {self.csv_file}")
                return False
            
            # Count rows
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f)
            
            print(f"📄 CSV file: {self.csv_file}")
            print(f"📊 Total rows: {row_count:,}")
            
            # Show sample data
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                first_row = next(reader)
                print(f"🔍 Sample row: {first_row}")
                print(f"📋 Column count: {len(first_row)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error checking CSV file: {e}")
            return False
    
    def get_table_info(self):
        """Get current table status"""
        try:
            cursor = self.conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'fahasa_dw' 
                AND table_name = 'date_dim'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if not table_exists:
                print("❌ Table date_dim does not exist in fahasa_dw")
                cursor.close()
                return False
            
            # Get current row count
            cursor.execute("SELECT COUNT(*) FROM date_dim")
            current_count = cursor.fetchone()[0]
            
            # Get table structure
            cursor.execute("DESCRIBE date_dim")
            columns = cursor.fetchall()
            
            print(f"📊 Current rows in date_dim: {current_count:,}")
            print(f"📋 Table structure: {len(columns)} columns")
            
            # Show column names
            column_names = [col[0] for col in columns]
            print(f"🏷️  Columns: {', '.join(column_names[:5])}...")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error getting table info: {e}")
            traceback.print_exc()
            return False
    
    def clear_existing_data(self):
        """Clear existing data from date_dim table"""
        try:
            cursor = self.conn.cursor()
            
            # Get current count
            cursor.execute("SELECT COUNT(*) FROM date_dim")
            current_count = cursor.fetchone()[0]
            
            if current_count > 0:
                print(f"🗑️  Clearing {current_count:,} existing records...")
                cursor.execute("DELETE FROM date_dim")
                print(f"✅ Cleared {cursor.rowcount:,} records")
                
                # Reset auto_increment
                cursor.execute("ALTER TABLE date_dim AUTO_INCREMENT = 1")
                print("🔄 Reset AUTO_INCREMENT to 1")
            else:
                print("📭 Table is already empty")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error clearing table: {e}")
            traceback.print_exc()
            return False
    
    def load_csv_data(self):
        """Load data from CSV to database"""
        try:
            cursor = self.conn.cursor()
            
            # Prepare INSERT statement - CSV columns match table structure
            insert_sql = """
            INSERT INTO date_dim (
                date_sk, full_date, day_since_2005, month_since_2005, 
                day_of_week, calendar_month, calendar_year, calendar_year_month,
                day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                week_sunday_start, week_of_year_monday, year_week_monday, 
                week_monday_start, holiday, day_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            print("📥 Loading CSV data...")
            
            # Read and insert data
            inserted_count = 0
            batch_size = 1000
            batch_data = []
            
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                
                for row_num, row in enumerate(csv_reader, 1):
                    try:
                        # Parse row data
                        if len(row) != 18:
                            print(f"⚠️  Row {row_num}: Invalid column count {len(row)}, expected 18")
                            continue
                        
                        # Convert data types
                        date_sk = int(row[0])
                        full_date = row[1]  # Already in YYYY-MM-DD format
                        day_since_2005 = int(row[2]) if row[2] else None
                        month_since_2005 = int(row[3]) if row[3] else None
                        day_of_week = row[4]
                        calendar_month = row[5]
                        calendar_year = int(row[6]) if row[6] else None
                        calendar_year_month = row[7]
                        day_of_month = int(row[8]) if row[8] else None
                        day_of_year = int(row[9]) if row[9] else None
                        week_of_year_sunday = int(row[10]) if row[10] else None
                        year_week_sunday = row[11]
                        week_sunday_start = row[12] if row[12] else None
                        week_of_year_monday = int(row[13]) if row[13] else None
                        year_week_monday = row[14]
                        week_monday_start = row[15] if row[15] else None
                        holiday = row[16] if row[16] else None
                        day_type = row[17]
                        
                        # Add to batch
                        batch_data.append((
                            date_sk, full_date, day_since_2005, month_since_2005,
                            day_of_week, calendar_month, calendar_year, calendar_year_month,
                            day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                            week_sunday_start, week_of_year_monday, year_week_monday,
                            week_monday_start, holiday, day_type
                        ))
                        
                        # Insert batch when full
                        if len(batch_data) >= batch_size:
                            cursor.executemany(insert_sql, batch_data)
                            inserted_count += len(batch_data)
                            batch_data.clear()
                            
                            if inserted_count % 5000 == 0:
                                print(f"   📊 Inserted {inserted_count:,} records...")
                    
                    except Exception as e:
                        print(f"⚠️  Row {row_num} error: {e}")
                        continue
                
                # Insert remaining batch
                if batch_data:
                    cursor.executemany(insert_sql, batch_data)
                    inserted_count += len(batch_data)
            
            print(f"✅ Successfully loaded {inserted_count:,} records")
            
            cursor.close()
            return inserted_count
            
        except Exception as e:
            print(f"❌ Error loading CSV data: {e}")
            traceback.print_exc()
            return 0
    
    def verify_data(self):
        """Verify loaded data"""
        try:
            cursor = self.conn.cursor()
            
            # Count total records
            cursor.execute("SELECT COUNT(*) FROM date_dim")
            total_count = cursor.fetchone()[0]
            
            # Get date range
            cursor.execute("""
                SELECT MIN(full_date), MAX(full_date)
                FROM date_dim
                WHERE full_date IS NOT NULL
            """)
            date_range = cursor.fetchone()
            
            # Sample verification
            cursor.execute("""
                SELECT 
                    calendar_year,
                    COUNT(*) as days_count
                FROM date_dim 
                GROUP BY calendar_year 
                ORDER BY calendar_year
                LIMIT 5
            """)
            year_counts = cursor.fetchall()
            
            print("\n📊 DATA VERIFICATION:")
            print(f"   📅 Total records: {total_count:,}")
            
            if date_range[0] and date_range[1]:
                print(f"   📆 Date range: {date_range[0]} to {date_range[1]}")
            
            if year_counts:
                print("   📈 Sample year counts:")
                for year, count in year_counts:
                    print(f"      {year}: {count} days")
            
            # Check for duplicates
            cursor.execute("""
                SELECT full_date, COUNT(*) as count
                FROM date_dim 
                GROUP BY full_date 
                HAVING COUNT(*) > 1
                LIMIT 5
            """)
            duplicates = cursor.fetchall()
            
            if duplicates:
                print("   ⚠️  Found duplicate dates:")
                for date, count in duplicates:
                    print(f"      {date}: {count} times")
            else:
                print("   ✅ No duplicate dates found")
            
            cursor.close()
            return total_count > 0
            
        except Exception as e:
            print(f"❌ Error verifying data: {e}")
            traceback.print_exc()
            return False
    
    def run_load(self):
        """Execute complete load process"""
        print("🗓️  FAHASA DATE DIMENSION LOADER")
        print("=" * 50)
        print(f"⏰ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        try:
            # Step 1: Check CSV file
            if not self.check_csv_file():
                return False
            
            # Step 2: Connect to database
            if not self.connect_database():
                return False
            
            # Step 3: Get table info
            if not self.get_table_info():
                return False
            
            # Step 4: Confirm operation
            response = input("\n❓ Clear existing data and load CSV? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("❌ Operation cancelled by user")
                return False
            
            # Begin transaction
            print("\n🔄 Starting transaction...")
            
            # Step 5: Clear existing data
            if not self.clear_existing_data():
                self.conn.rollback()
                return False
            
            # Step 6: Load CSV data
            inserted_count = self.load_csv_data()
            if inserted_count == 0:
                print("❌ No data loaded, rolling back...")
                self.conn.rollback()
                return False
            
            # Step 7: Verify loaded data
            if not self.verify_data():
                print("⚠️  Data verification failed, but continuing...")
            
            # Commit transaction
            self.conn.commit()
            print(f"\n✅ TRANSACTION COMMITTED!")
            
            print(f"\n🎉 DATE DIMENSION LOAD COMPLETED!")
            print(f"📊 Total records loaded: {inserted_count:,}")
            print(f"⏰ End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Load process failed: {e}")
            if self.conn:
                self.conn.rollback()
                print("🔄 Transaction rolled back")
            traceback.print_exc()
            return False
        finally:
            self.disconnect_database()

def main():
    """Main function for standalone execution"""
    print("🗓️  LOAD DATE DIMENSION FROM CSV")
    print("Loading date_dim.csv → fahasa_dw.date_dim")
    print()
    
    loader = DateDimLoader()
    success = loader.run_load()
    
    if success:
        print("\n✅ Date dimension loaded successfully!")
    else:
        print("\n❌ Date dimension load failed!")
    
    return success

if __name__ == "__main__":
    main()