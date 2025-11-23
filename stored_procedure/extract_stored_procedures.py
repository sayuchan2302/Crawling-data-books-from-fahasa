#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract Stored Procedures Script
Extract all stored procedures from 4 databases and create SQL files
"""

import mysql.connector
import os
from datetime import datetime

def extract_stored_procedures():
    """Extract stored procedures from all databases"""
    
    # Database connection config
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'charset': 'utf8mb4'
    }
    
    # Target databases
    databases = ['fahasa_staging', 'fahasa_dw', 'fahasa_datamart', 'fahasa_control']
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Create stored_procedure directory
        sp_dir = 'stored_procedure'
        os.makedirs(sp_dir, exist_ok=True)
        
        print("🔍 EXTRACTING STORED PROCEDURES FROM ALL DATABASES")
        print("=" * 60)
        
        total_procedures = 0
        
        for db in databases:
            try:
                print(f"\n📊 Processing Database: {db}")
                
                # Use database
                cursor.execute(f"USE `{db}`")
                
                # Get all stored procedures
                cursor.execute("SHOW PROCEDURE STATUS WHERE Db = %s", (db,))
                procedures = cursor.fetchall()
                
                print(f"   Found {len(procedures)} stored procedures")
                
                if len(procedures) > 0:
                    # Create file for this database
                    filename = os.path.join(sp_dir, f"{db}_stored_procedures.sql")
                    
                    with open(filename, 'w', encoding='utf-8') as f:
                        # Write header
                        f.write("-- " + "=" * 60 + "\n")
                        f.write(f"-- Stored Procedures for Database: {db}\n")
                        f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        f.write(f"-- Total procedures: {len(procedures)}\n")
                        f.write("-- " + "=" * 60 + "\n\n")
                        f.write(f"USE `{db}`;\n\n")
                        
                        # Extract each procedure
                        for proc_info in procedures:
                            proc_name = proc_info[1]  # Routine_name
                            proc_type = proc_info[2]  # Routine_type
                            created = proc_info[4]    # Created
                            modified = proc_info[5]   # Modified
                            
                            print(f"   - {proc_name}")
                            
                            try:
                                # Get procedure definition
                                cursor.execute(f"SHOW CREATE PROCEDURE `{proc_name}`")
                                result = cursor.fetchone()
                                
                                if result and len(result) > 2:
                                    procedure_def = result[2]  # Create Procedure column
                                    
                                    # Write procedure info
                                    f.write("-- " + "=" * 50 + "\n")
                                    f.write(f"-- Procedure: {proc_name}\n")
                                    f.write(f"-- Type: {proc_type}\n")
                                    f.write(f"-- Created: {created}\n")
                                    f.write(f"-- Modified: {modified}\n")
                                    f.write("-- " + "=" * 50 + "\n\n")
                                    
                                    # Drop and create
                                    f.write(f"DROP PROCEDURE IF EXISTS `{proc_name}`;\n\n")
                                    f.write("DELIMITER $$\n\n")
                                    f.write(procedure_def)
                                    f.write("\n$$\n\n")
                                    f.write("DELIMITER ;\n\n")
                                    f.write("-- " + "-" * 50 + "\n\n")
                                
                            except Exception as e:
                                print(f"     ⚠️ Error getting {proc_name}: {e}")
                                f.write(f"-- ERROR: Could not retrieve {proc_name}: {str(e)}\n\n")
                    
                    print(f"   ✅ Saved to: {filename}")
                    total_procedures += len(procedures)
                    
                else:
                    print(f"   📭 No procedures found in {db}")
                    
            except Exception as e:
                print(f"   ❌ Error processing {db}: {e}")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 EXTRACTION COMPLETED!")
        print(f"📊 Total procedures extracted: {total_procedures}")
        print(f"📁 Files saved in: {sp_dir}/")
        
        # List created files
        files = [f for f in os.listdir(sp_dir) if f.endswith('.sql')]
        if files:
            print(f"\n📄 Created files:")
            for file in sorted(files):
                file_path = os.path.join(sp_dir, file)
                size = os.path.getsize(file_path)
                print(f"   - {file} ({size:,} bytes)")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False

if __name__ == "__main__":
    extract_stored_procedures()