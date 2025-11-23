#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Install DataMart Stored Procedures
Execute DataMart stored procedures setup using Python MySQL connector
"""

import mysql.connector
import os

def install_stored_procedures():
    """Install all DataMart stored procedures for optimal performance"""
    
    # Database configuration
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'database': 'fahasa_datamart',
        'charset': 'utf8mb4'
    }
    
    try:
        print("🎯 INSTALLING DATAMART STORED PROCEDURES")
        print("=" * 50)
        
        # Connect to database
        print("🔄 Connecting to fahasa_datamart...")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        print("✅ Connected to fahasa_datamart")
        
        # Define stored procedures directly
        procedures = [
            {
                'name': 'sp_load_mart_author_insights',
                'sql': """
CREATE PROCEDURE sp_load_mart_author_insights()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
        VALUES ('ERROR', CONCAT('sp_load_mart_author_insights failed: ', @@error_count), NOW());
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', 'Starting sp_load_mart_author_insights', NOW());
    
    TRUNCATE TABLE mart_author_insights;
    
    INSERT INTO mart_author_insights (
        author_id, author_name, total_books, avg_rating, 
        total_sold, total_revenue, most_popular_book, last_updated
    )
    SELECT 
        ad.author_id,
        ad.author_name,
        COALESCE(COUNT(DISTINCT db.book_sk), 0) as total_books,
        COALESCE(ROUND(AVG(CASE WHEN fbs.rating > 0 THEN fbs.rating END), 2), 0) as avg_rating,
        COALESCE(SUM(fbs.sold_count_numeric), 0) as total_sold,
        COALESCE(SUM(fbs.original_price * fbs.sold_count_numeric), 0) as total_revenue,
        COALESCE((
            SELECT db2.title 
            FROM fahasa_dw.dim_books db2
            JOIN fahasa_dw.fact_book_sales fbs2 ON db2.book_sk = fbs2.book_sk
            WHERE db2.author LIKE CONCAT('%', ad.author_name, '%') 
               OR ad.author_name LIKE CONCAT('%', db2.author, '%')
            ORDER BY fbs2.sold_count_numeric DESC
            LIMIT 1
        ), 'No Books') as most_popular_book,
        NOW() as last_updated
    FROM fahasa_dw.author_dim ad
    LEFT JOIN fahasa_dw.dim_books db ON (
        db.author LIKE CONCAT('%', ad.author_name, '%') 
        OR ad.author_name LIKE CONCAT('%', db.author, '%')
    )
    LEFT JOIN fahasa_dw.fact_book_sales fbs ON db.book_sk = fbs.book_sk
    WHERE db.book_sk IS NOT NULL
    GROUP BY ad.author_id, ad.author_name
    HAVING total_books > 0
    ORDER BY total_revenue DESC;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', CONCAT('sp_load_mart_author_insights completed. Rows inserted: ', ROW_COUNT()), NOW());
    
    COMMIT;
END"""
            },
            {
                'name': 'sp_load_mart_category_performance',
                'sql': """
CREATE PROCEDURE sp_load_mart_category_performance()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
        VALUES ('ERROR', CONCAT('sp_load_mart_category_performance failed: ', @@error_count), NOW());
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', 'Starting sp_load_mart_category_performance', NOW());
    
    TRUNCATE TABLE mart_category_performance;
    
    INSERT INTO mart_category_performance (
        category_id, category_path, total_books, avg_rating,
        avg_price, total_sold, total_revenue, market_share, last_updated
    )
    SELECT 
        dc.category_sk as category_id,
        COALESCE(dc.category_path, 'Uncategorized') as category_path,
        COUNT(DISTINCT bbc.book_sk) as total_books,
        AVG(fbs.rating) as avg_rating,
        AVG(fbs.original_price) as avg_price,
        COALESCE(SUM(fbs.sold_count_numeric), 0) as total_sold,
        COALESCE(SUM(fbs.original_price * fbs.sold_count_numeric), 0) as total_revenue,
        COALESCE((SUM(fbs.original_price * fbs.sold_count_numeric) / NULLIF((
            SELECT SUM(original_price * sold_count_numeric) FROM fahasa_dw.fact_book_sales
        ), 0) * 100), 0) as market_share,
        NOW() as last_updated
    FROM fahasa_dw.dim_categories dc
    LEFT JOIN fahasa_dw.bridge_book_categories bbc ON dc.category_sk = bbc.category_sk
    LEFT JOIN fahasa_dw.fact_book_sales fbs ON bbc.book_sk = fbs.book_sk
    WHERE dc.category_path IS NOT NULL
    GROUP BY dc.category_sk, dc.category_path
    ORDER BY total_revenue DESC;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', CONCAT('sp_load_mart_category_performance completed. Rows inserted: ', ROW_COUNT()), NOW());
    
    COMMIT;
END"""
            },
            {
                'name': 'sp_load_mart_daily_sales',
                'sql': """
CREATE PROCEDURE sp_load_mart_daily_sales()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
        VALUES ('ERROR', CONCAT('sp_load_mart_daily_sales failed: ', @@error_count), NOW());
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', 'Starting sp_load_mart_daily_sales', NOW());
    
    TRUNCATE TABLE mart_daily_sales;
    
    INSERT INTO mart_daily_sales (
        date, product_id, product_name, category_path, publisher_name,
        author_names, price, discount_price, discount_percent, rating,
        rating_count, sold_today, sold_cumulative
    )
    SELECT DISTINCT
        dd.full_date as date,
        pd.id as product_id,
        COALESCE(pd.product_name, CONCAT('Product ', pd.id)) as product_name,
        COALESCE(dc.category_path, 'Uncategorized') as category_path,
        COALESCE(db.publisher, 'Unknown Publisher') as publisher_name,
        COALESCE(db.author, 'Unknown Author') as author_names,
        COALESCE(fbs.original_price, 0) as price,
        fbs.discount_price,
        fbs.discount_percent,
        fbs.rating,
        COALESCE(fbs.rating_count, 0) as rating_count,
        COALESCE(fbs.sold_count_numeric, 0) as sold_today,
        COALESCE(SUM(fbs.sold_count_numeric) OVER (
            PARTITION BY pd.id 
            ORDER BY dd.full_date 
            ROWS UNBOUNDED PRECEDING
        ), 0) as sold_cumulative
    FROM fahasa_dw.fact_book_sales fbs
    JOIN fahasa_dw.dim_date dd ON fbs.date_sk = dd.date_sk
    JOIN fahasa_dw.product_dim pd ON fbs.book_sk = pd.id
    LEFT JOIN fahasa_dw.dim_books db ON pd.id = db.book_sk
    LEFT JOIN fahasa_dw.bridge_book_categories bbc ON pd.id = bbc.book_sk
    LEFT JOIN fahasa_dw.dim_categories dc ON bbc.category_sk = dc.category_sk
    WHERE dd.full_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    ORDER BY dd.full_date DESC, pd.id;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', CONCAT('sp_load_mart_daily_sales completed. Rows inserted: ', ROW_COUNT()), NOW());
    
    COMMIT;
END"""
            },
            {
                'name': 'sp_load_mart_product_flat',
                'sql': """
CREATE PROCEDURE sp_load_mart_product_flat()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
        VALUES ('ERROR', CONCAT('sp_load_mart_product_flat failed: ', @@error_count), NOW());
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', 'Starting sp_load_mart_product_flat', NOW());
    
    TRUNCATE TABLE mart_product_flat;
    
    INSERT INTO mart_product_flat (
        product_id, product_name, isbn, category_path, author_names,
        publisher_name, page_count, weight, dimensions, language,
        first_seen, last_seen, total_sold, avg_rating, avg_price
    )
    SELECT 
        pd.id as product_id,
        COALESCE(pd.product_name, CONCAT('Product ', pd.id)) as product_name,
        pd.isbn,
        COALESCE(dc.category_path, 'Uncategorized') as category_path,
        COALESCE(db.author, 'Unknown Author') as author_names,
        COALESCE(db.publisher, 'Unknown Publisher') as publisher_name,
        pd.page_count,
        pd.weight,
        pd.dimensions,
        COALESCE(pd.language, 'Vietnamese') as language,
        MIN(dd.full_date) as first_seen,
        MAX(dd.full_date) as last_seen,
        COALESCE(SUM(fbs.sold_count_numeric), 0) as total_sold,
        AVG(fbs.rating) as avg_rating,
        AVG(fbs.original_price) as avg_price
    FROM fahasa_dw.product_dim pd
    LEFT JOIN fahasa_dw.dim_books db ON pd.id = db.book_sk
    LEFT JOIN fahasa_dw.bridge_book_categories bbc ON pd.id = bbc.book_sk
    LEFT JOIN fahasa_dw.dim_categories dc ON bbc.category_sk = dc.category_sk
    LEFT JOIN fahasa_dw.fact_book_sales fbs ON pd.id = fbs.book_sk
    LEFT JOIN fahasa_dw.dim_date dd ON fbs.date_sk = dd.date_sk
    GROUP BY pd.id, pd.product_name, pd.isbn, dc.category_path, 
             db.author, db.publisher, pd.page_count, pd.weight, 
             pd.dimensions, pd.language
    ORDER BY total_sold DESC;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', CONCAT('sp_load_mart_product_flat completed. Rows inserted: ', ROW_COUNT()), NOW());
    
    COMMIT;
END"""
            },
            {
                'name': 'sp_load_mart_publisher_performance',
                'sql': """
CREATE PROCEDURE sp_load_mart_publisher_performance()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
        VALUES ('ERROR', CONCAT('sp_load_mart_publisher_performance failed: ', @@error_count), NOW());
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', 'Starting sp_load_mart_publisher_performance', NOW());
    
    TRUNCATE TABLE mart_publisher_performance;
    
    INSERT INTO mart_publisher_performance (
        publisher_id, publisher_name, total_books, avg_rating,
        total_sold, total_revenue, market_share, last_updated
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(fbs.original_price * fbs.sold_count_numeric), 0) DESC) as publisher_id,
        COALESCE(db.publisher, 'Unknown Publisher') as publisher_name,
        COUNT(DISTINCT db.book_sk) as total_books,
        AVG(fbs.rating) as avg_rating,
        COALESCE(SUM(fbs.sold_count_numeric), 0) as total_sold,
        COALESCE(SUM(fbs.original_price * fbs.sold_count_numeric), 0) as total_revenue,
        COALESCE((SUM(fbs.original_price * fbs.sold_count_numeric) / NULLIF((
            SELECT SUM(original_price * sold_count_numeric) FROM fahasa_dw.fact_book_sales
        ), 0) * 100), 0) as market_share,
        NOW() as last_updated
    FROM fahasa_dw.dim_books db
    LEFT JOIN fahasa_dw.fact_book_sales fbs ON db.book_sk = fbs.book_sk
    WHERE db.publisher IS NOT NULL AND db.publisher != ''
    GROUP BY db.publisher
    HAVING total_books > 0
    ORDER BY total_revenue DESC;
    
    INSERT INTO fahasa_control.logs (log_level, error_message, create_time)
    VALUES ('INFO', CONCAT('sp_load_mart_publisher_performance completed. Rows inserted: ', ROW_COUNT()), NOW());
    
    COMMIT;
END"""
            }
        ]
        
        # Drop existing procedures and create new ones
        for procedure in procedures:
            print(f"🔄 Installing {procedure['name']}...")
            
            # Drop if exists
            cursor.execute(f"DROP PROCEDURE IF EXISTS {procedure['name']}")
            
            # Create procedure
            cursor.execute(procedure['sql'])
            conn.commit()
            
            print(f"✅ {procedure['name']} installed successfully")
        
        # Verify stored procedures were created
        print("\n🔍 VERIFYING INSTALLED PROCEDURES...")
        
        cursor.execute("""
            SELECT ROUTINE_NAME 
            FROM information_schema.ROUTINES 
            WHERE ROUTINE_SCHEMA = 'fahasa_datamart' 
            AND ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ROUTINE_NAME
        """)
        
        procedures_found = cursor.fetchall()
        
        if procedures_found:
            print(f"✅ Found {len(procedures_found)} stored procedures:")
            for proc in procedures_found:
                print(f"   📋 {proc[0]}")
        else:
            print("❌ No stored procedures found")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 INSTALLATION COMPLETED!")
        print(f"📊 Installed {len(procedures_found)} stored procedures")
        
        return len(procedures_found) > 0
        
    except Exception as e:
        print(f"❌ Installation failed: {e}")
        return False

def main():
    """Main function"""
    print("🎯 DATAMART STORED PROCEDURE INSTALLER")
    print()
    
    success = install_stored_procedures()
    
    if success:
        print(f"\n✅ Installation completed successfully!")
        print(f"🚀 Ready to run load_data_mart.py")
    else:
        print(f"\n❌ Installation failed!")
    
    return success

if __name__ == "__main__":
    main()