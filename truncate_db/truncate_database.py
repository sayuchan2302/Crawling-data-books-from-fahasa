#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Database Truncate Script - Version 2
Script đơn giản để truncate bất kỳ database nào
"""

import mysql.connector
import sys
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

def truncate_database(database_name):
    """Truncate tất cả tables trong database"""
    print(f"🗑️  Truncating database: {database_name}")
    
    connection, cursor = connect_db(database_name)
    if not connection:
        return False
    
    try:
        # Tắt Foreign Key Checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Lấy danh sách tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("ℹ️  Database rỗng!")
            return True
        
        print(f"📊 Tìm thấy {len(tables)} tables")
        
        total_deleted = 0
        success_count = 0
        
        # Truncate từng table
        for table in tables:
            try:
                # Đếm records
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Truncate
                cursor.execute(f"TRUNCATE TABLE {table}")
                
                total_deleted += count
                success_count += 1
                
                print(f"   ✅ {table}: {count:,} records deleted")
                
            except Exception as e:
                print(f"   ❌ {table}: {e}")
        
        # Reset AUTO_INCREMENT
        cursor.execute(f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{database_name}' 
            AND EXTRA = 'auto_increment'
        """)
        
        for (table_name,) in cursor.fetchall():
            cursor.execute(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
        
        # Bật lại Foreign Key Checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        connection.commit()
        
        print(f"\n📊 Kết quả:")
        print(f"   ✅ Success: {success_count}/{len(tables)} tables")
        print(f"   🗑️  Deleted: {total_deleted:,} records")
        
        return success_count == len(tables)
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False
    finally:
        cursor.close()
        connection.close()

def interactive_mode():
    """Chế độ tương tác để chọn database"""
    print("🗑️  INTERACTIVE DATABASE TRUNCATE")
    print("=" * 40)
    
    # Lấy danh sách databases
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='123456'
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        
        all_dbs = [row[0] for row in cursor.fetchall()]
        databases = [db for db in all_dbs if db not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
        
        cursor.close()
        conn.close()
        
        print("📋 Available databases:")
        for i, db in enumerate(databases, 1):
            print(f"   {i}. {db}")
        print("   0. Exit")
        
        while True:
            choice = input(f"\n❓ Chọn database (0-{len(databases)}): ").strip()
            
            if choice == '0':
                print("👋 Goodbye!")
                return None
            
            try:
                index = int(choice) - 1
                if 0 <= index < len(databases):
                    return databases[index]
                else:
                    print("❌ Số không hợp lệ!")
            except ValueError:
                print("❌ Vui lòng nhập số!")
                
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return None

def main():
    print("🗑️  UNIVERSAL DATABASE TRUNCATE")
    print("=" * 50)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Kiểm tra arguments
    if len(sys.argv) > 1:
        database_name = sys.argv[1]
    else:
        database_name = interactive_mode()
        if not database_name:
            return
    
    print(f"🎯 Target Database: {database_name}")
    
    # Xác nhận
    print(f"\n⚠️  CẢNH BÁO:")
    print(f"   Sẽ xóa TẤT CẢ dữ liệu trong database: {database_name}")
    print(f"   Hành động này KHÔNG THỂ HOÀN TÁC!")
    
    confirm = input(f"\n❓ Tiếp tục? (yes/no): ").strip().lower()
    
    if confirm not in ['yes', 'y']:
        print("❌ Đã hủy!")
        return
    
    # Truncate database
    print()
    success = truncate_database(database_name)
    
    if success:
        print(f"\n🎉 THÀNH CÔNG!")
        print(f"✅ Database {database_name} đã được làm sạch hoàn toàn!")
    else:
        print(f"\n❌ THẤT BẠI!")
        print(f"⚠️  Một số tables có thể chưa được truncate!")

if __name__ == "__main__":
    main()