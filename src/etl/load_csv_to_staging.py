"""
CSV to Staging Loader
Tải dữ liệu từ file CSV vào staging_books table
"""
import mysql.connector
import pandas as pd
import sys
import os
from datetime import datetime

# MySQL Configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'fahasa_staging',
    'charset': 'utf8mb4'
}

def get_mysql_connection():
    """Tạo kết nối MySQL"""
    return mysql.connector.connect(**MYSQL_CONFIG)

def load_csv_to_staging(csv_file_path):
    """Load CSV file vào staging_books table"""
    try:
        # Check if file exists
        if not os.path.exists(csv_file_path):
            print(f"❌ File không tồn tại: {csv_file_path}")
            return False
        
        # Read CSV
        print(f"📄 Đọc file CSV: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        print(f"📊 Tìm thấy {len(df)} sách trong CSV")
        
        if len(df) == 0:
            print("⚠️ File CSV trống")
            return True
        
        # Connect to MySQL
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        # Prepare insert statement
        insert_sql = '''
            INSERT INTO staging_books (
                title, author, publisher, supplier,
                category_1, category_2, category_3,
                original_price, discount_price, discount_percent,
                rating, rating_count, sold_count, sold_count_numeric,
                publish_year, language, page_count, weight, dimensions,
                url, url_img, time_collect
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        '''
        
        success_count = 0
        error_count = 0
        
        for idx, row in df.iterrows():
            try:
                # Convert time_collect if needed
                time_collect = row.get('time_collect', datetime.now())
                if isinstance(time_collect, str):
                    try:
                        time_collect = datetime.strptime(time_collect, '%Y-%m-%d %H:%M:%S')
                    except:
                        time_collect = datetime.now()
                
                cursor.execute(insert_sql, (
                    str(row.get('title', '')),
                    str(row.get('author', '')),
                    str(row.get('publisher', '')),
                    str(row.get('supplier', '')),
                    str(row.get('category_1', '')),
                    str(row.get('category_2', '')),
                    str(row.get('category_3', '')),
                    float(row.get('original_price', 0)),
                    float(row.get('discount_price', 0)),
                    float(row.get('discount_percent', 0)),
                    float(row.get('rating', 0)),
                    int(row.get('rating_count', 0)),
                    str(row.get('sold_count', '')),
                    int(row.get('sold_count_numeric', 0)),
                    int(row.get('publish_year', 2025)) if pd.notna(row.get('publish_year')) else None,
                    str(row.get('language', '')),
                    int(row.get('page_count', 0)) if pd.notna(row.get('page_count')) else None,
                    float(row.get('weight', 0)) if pd.notna(row.get('weight')) else None,
                    str(row.get('dimensions', '')),
                    str(row.get('url', '')),
                    str(row.get('url_img', '')),
                    time_collect
                ))
                success_count += 1
                
            except Exception as e:
                print(f"❌ Lỗi insert row {idx + 1}: {e}")
                error_count += 1
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n✅ HOÀN TẤT LOAD CSV!")
        print(f"📊 Thành công: {success_count} sách")
        print(f"❌ Lỗi: {error_count} sách")
        print(f"📁 File: {csv_file_path}")
        
        return success_count > 0
        
    except Exception as e:
        print(f"❌ Lỗi load CSV: {e}")
        return False

def get_latest_csv_file():
    """Tìm file CSV mới nhất trong folder backup"""
    try:
        now = datetime.now()
        # Get correct path to data folder
        script_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        backup_dir = os.path.join(
            script_dir, 
            'data', str(now.year), f"{now.month:02d}", f"{now.day:02d}"
        )
        
        if not os.path.exists(backup_dir):
            print(f"❌ Folder backup không tồn tại: {backup_dir}")
            return None
        
        # Find all CSV files
        csv_files = [f for f in os.listdir(backup_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"❌ Không tìm thấy file CSV trong: {backup_dir}")
            return None
        
        # Get latest CSV file
        csv_files.sort(reverse=True)
        latest_csv = os.path.join(backup_dir, csv_files[0])
        
        print(f"🔍 File CSV mới nhất: {latest_csv}")
        return latest_csv
        
    except Exception as e:
        print(f"❌ Lỗi tìm file CSV: {e}")
        return None

def main():
    """Main function"""
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        if not os.path.isabs(csv_file):
            # Convert relative path to absolute
            csv_file = os.path.abspath(csv_file)
    else:
        # Find latest CSV file
        csv_file = get_latest_csv_file()
        
    if not csv_file:
        print("❌ Không tìm thấy file CSV để load")
        return False
    
    return load_csv_to_staging(csv_file)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)