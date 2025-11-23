#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Truncate Fahasa Staging Database Script
Xóa tất cả dữ liệu trong database staging để chuẩn bị cho ETL mới
"""

import mysql.connector
import traceback
from datetime import datetime

def connect_db():
    """Kết nối đến MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='123456',
            database='fahasa_staging',
            charset='utf8mb4'
        )
        return connection, connection.cursor()
    except Exception as e:
        print(f"❌ Kết nối database thất bại: {e}")
        return None, None

def get_staging_tables():
    """Lấy danh sách tất cả tables trong staging database"""
    print("🔍 KIỂM TRA CÁC TABLES TRONG STAGING DATABASE...")
    
    connection, cursor = connect_db()
    if not connection:
        return []
    
    try:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"📋 Tìm thấy {len(tables)} tables trong fahasa_staging:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   📊 {table}: {count:,} records")
        
        return tables
        
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra tables: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def backup_staging_data():
    """Backup dữ liệu staging trước khi truncate (tùy chọn)"""
    print("\n💾 TẠO BACKUP DỮ LIỆU STAGING...")
    
    connection, cursor = connect_db()
    if not connection:
        return False
    
    try:
        # Tạo backup database
        backup_name = f"fahasa_staging_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Lưu thông tin backup
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        total_records = 0
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            total_records += count
        
        print(f"📊 Tổng {total_records:,} records sẽ được backup")
        print(f"💾 Backup name: {backup_name}")
        print(f"ℹ️  Lưu ý: Backup thực tế cần tools như mysqldump")
        
        return True
        
    except Exception as e:
        print(f"❌ Lỗi backup: {e}")
        return False
    finally:
        cursor.close()
        connection.close()

def disable_foreign_key_checks():
    """Tạm thời tắt foreign key checks để truncate an toàn"""
    connection, cursor = connect_db()
    if not connection:
        return None, None
    
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        print("🔧 Đã tắt Foreign Key Checks")
        return connection, cursor
    except Exception as e:
        print(f"❌ Lỗi tắt FK checks: {e}")
        cursor.close()
        connection.close()
        return None, None

def truncate_staging_tables(tables):
    """Truncate tất cả tables trong staging"""
    print(f"\n🗑️  TRUNCATE STAGING TABLES...")
    
    connection, cursor = disable_foreign_key_checks()
    if not connection:
        return False
    
    try:
        truncated_tables = []
        total_deleted = 0
        
        for table in tables:
            try:
                # Đếm records trước khi truncate
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                before_count = cursor.fetchone()[0]
                
                # Truncate table
                cursor.execute(f"TRUNCATE TABLE {table}")
                
                # Verify truncate
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                after_count = cursor.fetchone()[0]
                
                truncated_tables.append(table)
                total_deleted += before_count
                
                print(f"   ✅ {table}: Xóa {before_count:,} → {after_count} records")
                
            except Exception as e:
                print(f"   ❌ {table}: Lỗi - {e}")
        
        # Bật lại Foreign Key Checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("🔧 Đã bật lại Foreign Key Checks")
        
        connection.commit()
        
        print(f"\n📊 KẾT QUÁ TRUNCATE:")
        print(f"   ✅ Tables truncated: {len(truncated_tables)}/{len(tables)}")
        print(f"   🗑️  Total records deleted: {total_deleted:,}")
        
        return len(truncated_tables) == len(tables)
        
    except Exception as e:
        print(f"❌ Lỗi truncate: {e}")
        traceback.print_exc()
        return False
    finally:
        cursor.close()
        connection.close()

def verify_truncate():
    """Kiểm tra kết quả truncate"""
    print(f"\n🔍 KIỂM TRA KẾT QUẢ TRUNCATE...")
    
    connection, cursor = connect_db()
    if not connection:
        return False
    
    try:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        all_empty = True
        total_remaining = 0
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_remaining += count
            
            if count == 0:
                print(f"   ✅ {table}: EMPTY")
            else:
                print(f"   ⚠️ {table}: {count} records còn lại")
                all_empty = False
        
        print(f"\n📊 TỔNG KẾT:")
        print(f"   📋 Total tables: {len(tables)}")
        print(f"   🗑️  Total remaining records: {total_remaining}")
        print(f"   ✅ All empty: {'YES' if all_empty else 'NO'}")
        
        return all_empty
        
    except Exception as e:
        print(f"❌ Lỗi verify: {e}")
        return False
    finally:
        cursor.close()
        connection.close()

def reset_auto_increment():
    """Reset AUTO_INCREMENT cho các tables"""
    print(f"\n🔄 RESET AUTO_INCREMENT...")
    
    connection, cursor = connect_db()
    if not connection:
        return False
    
    try:
        # Lấy tables có AUTO_INCREMENT
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'fahasa_staging' 
            AND EXTRA = 'auto_increment'
        """)
        
        auto_inc_tables = cursor.fetchall()
        
        for table_name, column_name in auto_inc_tables:
            cursor.execute(f"ALTER TABLE {table_name} AUTO_INCREMENT = 1")
            print(f"   🔄 {table_name}.{column_name}: Reset to 1")
        
        connection.commit()
        print(f"✅ Reset {len(auto_inc_tables)} AUTO_INCREMENT columns")
        
        return True
        
    except Exception as e:
        print(f"❌ Lỗi reset AUTO_INCREMENT: {e}")
        return False
    finally:
        cursor.close()
        connection.close()

def main():
    print("🗑️  FAHASA STAGING DATABASE TRUNCATE SCRIPT")
    print("=" * 60)
    print(f"⏰ Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Bước 1: Kiểm tra tables
    tables = get_staging_tables()
    if not tables:
        print("❌ Không tìm thấy tables hoặc có lỗi!")
        return
    
    # Bước 2: Xác nhận từ user
    print(f"\n⚠️  CẢNH BÁO:")
    print(f"   Bạn sắp xóa TẤT CẢ dữ liệu trong {len(tables)} tables!")
    print(f"   Hành động này KHÔNG THỂ HOÀN TÁC!")
    
    confirm = input(f"\n❓ Bạn có chắc chắn muốn tiếp tục? (yes/no): ").strip().lower()
    
    if confirm not in ['yes', 'y']:
        print("❌ Hủy bỏ truncate operation")
        return
    
    # Bước 3: Backup (tùy chọn)
    backup_confirm = input("❓ Bạn có muốn tạo backup trước? (yes/no): ").strip().lower()
    if backup_confirm in ['yes', 'y']:
        backup_staging_data()
    
    # Bước 4: Truncate tables
    success = truncate_staging_tables(tables)
    
    if success:
        # Bước 5: Reset AUTO_INCREMENT
        reset_auto_increment()
        
        # Bước 6: Verify kết quả
        verify_success = verify_truncate()
        
        if verify_success:
            print(f"\n🎉 TRUNCATE THÀNH CÔNG!")
            print(f"✅ Fahasa staging database đã được làm sạch hoàn toàn")
            print(f"🚀 Sẵn sàng cho ETL process mới!")
        else:
            print(f"\n⚠️ Truncate hoàn thành nhưng một số tables vẫn có dữ liệu")
    else:
        print(f"\n❌ Truncate thất bại!")

if __name__ == "__main__":
    main()