#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Database Truncate Script
Script tổng quát để truncate bất kỳ database nào
"""

import mysql.connector
import argparse
import traceback
from datetime import datetime

def connect_db(database_name):
    """Kết nối đến MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='123456',
            database=database_name,
            charset='utf8mb4'
        )
        return connection, connection.cursor()
    except Exception as e:
        print(f"❌ Kết nối {database_name} thất bại: {e}")
        return None, None

def get_database_info(database_name):
    """Lấy thông tin về database và tables"""
    print(f"🔍 KIỂM TRA DATABASE: {database_name.upper()}")
    print("-" * 50)
    
    connection, cursor = connect_db(database_name)
    if not connection:
        return None
    
    try:
        # Kiểm tra database có tồn tại không
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]
        
        if current_db != database_name:
            print(f"❌ Database {database_name} không tồn tại!")
            return None
        
        # Lấy danh sách tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print(f"ℹ️  Database {database_name} không có tables!")
            return {"database": database_name, "tables": [], "total_records": 0}
        
        # Đếm records trong từng table
        table_info = []
        total_records = 0
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_records += count
                
                table_info.append({
                    "name": table,
                    "records": count
                })
                
                print(f"   📊 {table}: {count:,} records")
                
            except Exception as e:
                print(f"   ❌ {table}: Lỗi đếm records - {e}")
                table_info.append({
                    "name": table,
                    "records": 0,
                    "error": str(e)
                })
        
        print(f"📊 Tổng: {len(tables)} tables, {total_records:,} records")
        
        return {
            "database": database_name,
            "tables": table_info,
            "total_records": total_records
        }
        
    except Exception as e:
        print(f"❌ Lỗi kiểm tra database: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def truncate_database(database_name, exclude_tables=None, include_tables=None):
    """Truncate database với tùy chọn exclude/include tables"""
    print(f"\n🗑️  TRUNCATE DATABASE: {database_name.upper()}")
    print("-" * 50)
    
    exclude_tables = exclude_tables or []
    include_tables = include_tables or []
    
    connection, cursor = connect_db(database_name)
    if not connection:
        return False
    
    try:
        # Tắt Foreign Key Checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        print("🔧 Đã tắt Foreign Key Checks")
        
        # Lấy danh sách tables
        cursor.execute("SHOW TABLES")
        all_tables = [row[0] for row in cursor.fetchall()]
        
        # Lọc tables cần truncate
        if include_tables:
            tables_to_truncate = [t for t in all_tables if t in include_tables]
        else:
            tables_to_truncate = [t for t in all_tables if t not in exclude_tables]
        
        if not tables_to_truncate:
            print("ℹ️  Không có tables nào để truncate!")
            return True
        
        print(f"🎯 Sẽ truncate {len(tables_to_truncate)} tables:")
        for table in tables_to_truncate:
            print(f"   - {table}")
        
        # Truncate từng table
        success_count = 0
        total_deleted = 0
        
        for table in tables_to_truncate:
            try:
                # Đếm records trước khi truncate
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                before_count = cursor.fetchone()[0]
                
                # Truncate table
                cursor.execute(f"TRUNCATE TABLE {table}")
                
                # Verify
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                after_count = cursor.fetchone()[0]
                
                success_count += 1
                total_deleted += before_count
                
                print(f"   ✅ {table}: {before_count:,} → {after_count} records")
                
            except Exception as e:
                print(f"   ❌ {table}: Lỗi - {e}")
        
        # Bật lại Foreign Key Checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("🔧 Đã bật lại Foreign Key Checks")
        
        # Reset AUTO_INCREMENT
        print("\n🔄 RESET AUTO_INCREMENT...")
        cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{database_name}' 
            AND EXTRA = 'auto_increment'
            AND TABLE_NAME IN ({','.join([f"'{t}'" for t in tables_to_truncate])})
        """)
        
        auto_inc_tables = cursor.fetchall()
        for table_name, column_name in auto_inc_tables:
            cursor.execute(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
            print(f"   🔄 {table_name}.{column_name}: Reset to 1")
        
        connection.commit()
        
        print(f"\n📊 KẾT QUÁ:")
        print(f"   ✅ Tables truncated: {success_count}/{len(tables_to_truncate)}")
        print(f"   🗑️  Total records deleted: {total_deleted:,}")
        print(f"   🔄 AUTO_INCREMENT reset: {len(auto_inc_tables)} columns")
        
        return success_count == len(tables_to_truncate)
        
    except Exception as e:
        print(f"❌ Lỗi truncate: {e}")
        traceback.print_exc()
        return False
    finally:
        cursor.close()
        connection.close()

def main():
    parser = argparse.ArgumentParser(description='Universal Database Truncate Script')
    parser.add_argument('database', help='Tên database cần truncate')
    parser.add_argument('--exclude', nargs='*', help='Danh sách tables cần loại trừ', default=[])
    parser.add_argument('--include', nargs='*', help='Chỉ truncate các tables này', default=[])
    parser.add_argument('--confirm', action='store_true', help='Bỏ qua xác nhận (tự động yes)')
    
    args = parser.parse_args()
    
    print("🗑️  UNIVERSAL DATABASE TRUNCATE SCRIPT")
    print("=" * 60)
    print(f"⏰ Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Target Database: {args.database}")
    
    if args.exclude:
        print(f"🚫 Exclude Tables: {', '.join(args.exclude)}")
    if args.include:
        print(f"✅ Include Tables: {', '.join(args.include)}")
    print()
    
    # Bước 1: Kiểm tra database
    db_info = get_database_info(args.database)
    if not db_info:
        print("❌ Không thể truy cập database!")
        return
    
    if db_info["total_records"] == 0:
        print("ℹ️  Database đã rỗng!")
        return
    
    # Bước 2: Xác nhận
    if not args.confirm:
        print(f"\n⚠️  CẢNH BÁO:")
        print(f"   Database: {args.database}")
        print(f"   Tables: {len(db_info['tables'])}")
        print(f"   Total Records: {db_info['total_records']:,}")
        print(f"   Hành động này KHÔNG THỂ HOÀN TÁC!")
        
        confirm = input(f"\n❓ Bạn có chắc chắn muốn tiếp tục? (yes/no): ").strip().lower()
        
        if confirm not in ['yes', 'y']:
            print("❌ Hủy bỏ truncate operation")
            return
    
    # Bước 3: Truncate
    success = truncate_database(
        args.database,
        exclude_tables=args.exclude,
        include_tables=args.include
    )
    
    if success:
        print(f"\n🎉 TRUNCATE THÀNH CÔNG!")
        print(f"✅ Database {args.database} đã được làm sạch!")
        print(f"🚀 Sẵn sàng cho dữ liệu mới!")
    else:
        print(f"\n❌ Truncate thất bại!")

if __name__ == "__main__":
    # Nếu chạy trực tiếp không có arguments, sử dụng interactive mode
    import sys
    if len(sys.argv) == 1:
        print("🗑️  INTERACTIVE MODE")
        print("=" * 30)
        
        # Hiển thị danh sách databases
        try:
            conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='123456'
            )
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() if row[0] not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
            
            print("📋 Available databases:")
            for i, db in enumerate(databases, 1):
                print(f"   {i}. {db}")
            
            cursor.close()
            conn.close()
            
            # User chọn database
            choice = input(f"\n❓ Chọn database (1-{len(databases)}): ").strip()
            try:
                db_index = int(choice) - 1
                selected_db = databases[db_index]
                sys.argv = [sys.argv[0], selected_db]
            except (ValueError, IndexError):
                print("❌ Lựa chọn không hợp lệ!")
                return
                
        except Exception as e:
            print(f"❌ Không thể lấy danh sách databases: {e}")
            return
    
    main()