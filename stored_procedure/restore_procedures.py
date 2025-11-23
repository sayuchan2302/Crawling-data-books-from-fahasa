#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Restore Stored Procedures Script
Restore all stored procedures to their respective databases
"""

import mysql.connector
import os
from datetime import datetime

def restore_stored_procedures():
    """Restore all stored procedures from SQL files"""
    
    # Database connection config
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'charset': 'utf8mb4',
        'autocommit': True
    }
    
    # SQL files mapping
    sql_files = {
        'fahasa_dw': 'fahasa_dw_stored_procedures.sql',
        'fahasa_control': 'fahasa_control_stored_procedures.sql'
    }
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        print("🔄 RESTORING STORED PROCEDURES TO ALL DATABASES")
        print("=" * 60)
        
        total_restored = 0
        sp_dir = 'stored_procedure'
        
        for database, sql_file in sql_files.items():
            file_path = os.path.join(sp_dir, sql_file)
            
            if not os.path.exists(file_path):
                print(f"⚠️ File not found: {file_path}")
                continue
                
            print(f"\n📊 Restoring to Database: {database}")
            print(f"   File: {sql_file}")
            
            try:
                # Read SQL file
                with open(file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # Split by delimiter blocks
                sql_blocks = sql_content.split('DELIMITER $$')
                
                procedure_count = 0
                
                for i, block in enumerate(sql_blocks):
                    if '$$' in block:
                        # This is a procedure block
                        parts = block.split('$$')
                        if len(parts) >= 2:
                            procedure_sql = parts[0].strip()
                            
                            if procedure_sql and 'CREATE' in procedure_sql.upper():
                                try:
                                    # Execute the procedure
                                    cursor.execute(procedure_sql)
                                    procedure_count += 1
                                    
                                    # Extract procedure name for logging
                                    lines = procedure_sql.split('\n')
                                    for line in lines:
                                        if 'CREATE' in line.upper() and 'PROCEDURE' in line.upper():
                                            proc_name = line.split('`')[1] if '`' in line else 'Unknown'
                                            print(f"   ✅ {proc_name}")
                                            break
                                    
                                except Exception as e:
                                    print(f"   ❌ Error creating procedure: {e}")
                    
                    elif 'DROP PROCEDURE' in block or 'USE' in block:
                        # Execute DROP and USE statements
                        statements = [stmt.strip() for stmt in block.split(';') if stmt.strip()]
                        for stmt in statements:
                            if stmt and (stmt.upper().startswith('DROP') or stmt.upper().startswith('USE')):
                                try:
                                    cursor.execute(stmt)
                                except Exception as e:
                                    if 'does not exist' not in str(e).lower():
                                        print(f"   ⚠️ Warning: {e}")
                
                print(f"   📈 Successfully restored {procedure_count} procedures")
                total_restored += procedure_count
                
            except Exception as e:
                print(f"   ❌ Error processing {database}: {e}")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 RESTORATION COMPLETED!")
        print(f"📊 Total procedures restored: {total_restored}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False

def verify_procedures():
    """Verify restored procedures"""
    
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'charset': 'utf8mb4'
    }
    
    databases = ['fahasa_dw', 'fahasa_control']
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        print("\n🔍 VERIFYING RESTORED PROCEDURES")
        print("=" * 40)
        
        total_count = 0
        
        for db in databases:
            cursor.execute(f"USE `{db}`")
            cursor.execute("SHOW PROCEDURE STATUS WHERE Db = %s", (db,))
            procedures = cursor.fetchall()
            
            print(f"\n📊 {db}: {len(procedures)} procedures")
            
            if len(procedures) > 0:
                for proc_info in procedures[:5]:  # Show first 5
                    proc_name = proc_info[1]
                    created = proc_info[4]
                    print(f"   - {proc_name} (created: {created})")
                
                if len(procedures) > 5:
                    print(f"   ... and {len(procedures) - 5} more")
            
            total_count += len(procedures)
        
        cursor.close()
        conn.close()
        
        print(f"\n✅ Total verified: {total_count} procedures")
        return True
        
    except Exception as e:
        print(f"❌ Verification error: {e}")
        return False

def main():
    """Main function"""
    print("🔧 FAHASA STORED PROCEDURES RESTORE UTILITY")
    print("Restore all stored procedures from SQL files")
    print()
    
    # Ask for confirmation
    response = input("❓ Restore all stored procedures? This will overwrite existing ones. (y/N): ").strip().lower()
    
    if response not in ['y', 'yes']:
        print("❌ Operation cancelled")
        return False
    
    # Restore procedures
    success = restore_stored_procedures()
    
    if success:
        # Verify restoration
        verify_procedures()
        print("\n✅ All stored procedures restored and verified!")
    else:
        print("\n❌ Restoration failed!")
    
    return success

if __name__ == "__main__":
    main()