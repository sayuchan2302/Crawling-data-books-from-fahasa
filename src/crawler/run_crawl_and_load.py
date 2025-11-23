#!/usr/bin/env python3
"""
Full Crawler and Load Workflow
1. Chạy crawler → tạo CSV/JSON files
2. Load CSV vào staging table
"""
import subprocess
import sys
import os
from datetime import datetime

def run_command(command, description):
    """Chạy command và hiển thị kết quả"""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    
    try:
        # Set UTF-8 encoding for subprocess
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=300,
            env=env,
            encoding='utf-8'
        )
        
        if result.stdout:
            print("📤 OUTPUT:")
            print(result.stdout)
        
        if result.stderr and result.returncode != 0:
            print("📥 STDERR:")
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"✅ {description} - THÀNH CÔNG")
            return True
        else:
            print(f"❌ {description} - THẤT BẠI (exit code: {result.returncode})")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - TIMEOUT (5 phút)")
        return False
    except Exception as e:
        print(f"❌ Lỗi chạy command: {e}")
        return False

def main():
    """Main workflow"""
    print("🔥 FAHASA DATA WAREHOUSE - CLEAN WORKFLOW")
    print("=" * 60)
    print("📋 WORKFLOW:")
    print("   1️⃣ Crawler → Tạo CSV/JSON files")
    print("   2️⃣ CSV Loader → Load vào staging table")
    print("=" * 60)
    
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    print(f"📍 Working directory: {os.getcwd()}")
    
    # Step 1: Run Crawler (file-only mode)
    print("\n🕷️ BƯỚC 1: Chỉ tạo files, không insert database")
    crawler_success = run_command(
        "python src/crawler/fahasa_bulk_scraper.py",
        "Crawler - Tạo CSV/JSON files"
    )
    
    if not crawler_success:
        print("❌ Crawler thất bại, dừng workflow")
        return False
    
    # Step 2: Load CSV to Staging
    print("\n📊 BƯỚC 2: Load CSV vào staging table")
    load_success = run_command(
        "python src/etl/load_csv_to_staging.py",
        "Load CSV → MySQL staging_books"
    )
    
    if not load_success:
        print("❌ Load CSV thất bại")
        return False
    
    # Step 3: Show Summary
    print(f"\n{'='*60}")
    print("📊 TÓM TẮT CLEAN WORKFLOW")
    print(f"{'='*60}")
    
    # Get current file info
    now = datetime.now()
    backup_dir = os.path.join('data', str(now.year), f"{now.month:02d}", f"{now.day:02d}")
    
    if os.path.exists(backup_dir):
        csv_files = [f for f in os.listdir(backup_dir) if f.endswith('.csv')]
        json_files = [f for f in os.listdir(backup_dir) if f.endswith('.json')]
        
        if csv_files:
            csv_files.sort(reverse=True)
            json_files.sort(reverse=True)
            print(f"📁 Files created:")
            print(f"   CSV: {os.path.join(backup_dir, csv_files[0])}")
            print(f"   JSON: {os.path.join(backup_dir, json_files[0])}")
    
    print("\n✅ CLEAN WORKFLOW HOÀN THÀNH!")
    print("\n🔄 WORKFLOW TIẾP THEO:")
    print("   3️⃣ ETL: staging → Data Warehouse")
    print("   4️⃣ Aggregate → datamart")
    print("   5️⃣ Data quality checks")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)