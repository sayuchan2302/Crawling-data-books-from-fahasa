#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Truncate Script - Truncate nhanh các database phổ biến
"""

import mysql.connector
from datetime import datetime

def quick_truncate(database_name):
    """Truncate nhanh một database"""
    try:
        print(f"🗑️  Quick Truncate: {database_name}")
        
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='123456',
            database=database_name
        )
        cursor = conn.cursor()
        
        # Tắt FK checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Lấy tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        total_deleted = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_deleted += count
            
            cursor.execute(f"TRUNCATE TABLE {table}")
            print(f"   ✅ {table}: {count:,} records deleted")
        
        # Reset AUTO_INCREMENT
        cursor.execute(f"""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{database_name}' 
            AND EXTRA = 'auto_increment'
        """)
        for (table_name,) in cursor.fetchall():
            cursor.execute(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
        
        # Bật lại FK checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Hoàn thành! Deleted {total_deleted:,} records từ {len(tables)} tables")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False

def main():
    """Menu chọn database để truncate"""
    print("⚡ QUICK TRUNCATE SCRIPT")
    print("=" * 40)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    databases = {
        "1": "fahasa_staging",
        "2": "fahasa_dw", 
        "3": "fahasa_datamart",
        "4": "fahasa_control"
    }
    
    print("📋 Chọn database để truncate:")
    for key, db in databases.items():
        print(f"   {key}. {db}")
    print("   0. Thoát")
    
    choice = input(f"\n❓ Lựa chọn (0-{len(databases)}): ").strip()
    
    if choice == "0":
        print("👋 Bye!")
        return
    
    if choice not in databases:
        print("❌ Lựa chọn không hợp lệ!")
        return
    
    selected_db = databases[choice]
    
    print(f"\n⚠️  CẢNH BÁO: Sẽ xóa TẤT CẢ dữ liệu trong {selected_db}!")
    confirm = input("❓ Tiếp tục? (yes/no): ").strip().lower()
    
    if confirm in ['yes', 'y']:
        print()
        success = quick_truncate(selected_db)
        
        if success:
            print(f"\n🎉 {selected_db} đã được làm sạch!")
        else:
            print(f"\n❌ Truncate {selected_db} thất bại!")
    else:
        print("❌ Đã hủy!")

if __name__ == "__main__":
    main()