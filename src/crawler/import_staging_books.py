import psycopg2
import pandas as pd
from psycopg2 import sql
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'postgres'
PG_PASSWORD = 'your_password'
PG_DATABASE = 'fahasa_staging'
CSV_PATH = 'd:/Project_DW/script/fahasa_complete_books.csv'
CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS staging_books (
    title TEXT,
    author TEXT,
    publisher TEXT,
    supplier TEXT,
    category_1 TEXT,
    category_2 TEXT,
    category_3 TEXT,
    original_price NUMERIC,
    discount_price NUMERIC,
    discount_percent NUMERIC,
    rating NUMERIC,
    rating_count INTEGER,
    sold_count TEXT,
    sold_count_numeric INTEGER,
    publish_year INTEGER,
    language TEXT,
    page_count INTEGER,
    weight NUMERIC,
    dimensions TEXT,
    url TEXT,
    url_img TEXT,
    time_collect TIMESTAMP
);
'''

def main():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE
    )
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print('Đã tạo bảng staging_books (nếu chưa có)')
    df = pd.read_csv(CSV_PATH)
    print(f'Đã đọc {len(df)} dòng từ file CSV')
    cur.execute('TRUNCATE TABLE staging_books;')
    conn.commit()
    print('Đã làm sạch bảng staging_books')
    import io
    output = io.StringIO()
    df.to_csv(output, sep='|', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'staging_books', sep='|')
    conn.commit()
    print('Đã import dữ liệu vào staging_books thành công!')
    cur.execute('SELECT COUNT(*) FROM staging_books;')
    count = cur.fetchone()[0]
    print(f'Số dòng trong staging_books: {count}')
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
