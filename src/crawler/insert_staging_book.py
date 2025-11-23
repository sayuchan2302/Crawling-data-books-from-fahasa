import mysql.connector
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
    config = MYSQL_CONFIG.copy()
    return mysql.connector.connect(**config)

def insert_book_staging(book_data):
    """Insert book data into MySQL staging_books table"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
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
        
        cursor.execute(insert_sql, (
            book_data.get('title', ''),
            book_data.get('author', ''),
            book_data.get('publisher', ''),
            book_data.get('supplier', ''),
            book_data.get('category_1', ''),
            book_data.get('category_2', ''),
            book_data.get('category_3', ''),
            book_data.get('original_price', 0),
            book_data.get('discount_price', 0),
            book_data.get('discount_percent', 0),
            book_data.get('rating', 0),
            book_data.get('rating_count', 0),
            book_data.get('sold_count', ''),
            book_data.get('sold_count_numeric', 0),
            book_data.get('publish_year', None),
            book_data.get('language', ''),
            book_data.get('page_count', None),
            book_data.get('weight', None),
            book_data.get('dimensions', ''),
            book_data.get('url', ''),
            book_data.get('url_img', ''),
            datetime.now()
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error inserting book data: {e}")
        return False

def test_mysql_connection():
    """Test MySQL connection"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM staging_books")
        count = cursor.fetchone()[0]
        print(f'✓ MySQL connection successful! Version: {version}')
        print(f'✓ Current staging records: {count}')
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f'✗ MySQL connection failed: {e}')
        return False

def bulk_insert_books(books_list):
    """Bulk insert multiple books"""
    success_count = 0
    for book in books_list:
        if insert_book_staging(book):
            success_count += 1
    return success_count

if __name__ == "__main__":
    test_mysql_connection()
