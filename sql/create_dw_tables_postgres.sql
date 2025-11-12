-- SQL script to create Data Warehouse tables (Star Schema) for Fahasa project
-- ERD chuẩn hóa, dùng _key, bổ sung các trường tổng hợp, log, v.v.

-- Dimension tables
CREATE TABLE IF NOT EXISTS dim_author (
    author_key SERIAL PRIMARY KEY,
    author_id VARCHAR,
    author_name VARCHAR,
    author_name_normalized VARCHAR,
    total_books INTEGER,
    total_sold INTEGER,
    avg_rating DECIMAL(3,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_publisher (
    publisher_key SERIAL PRIMARY KEY,
    publisher_id VARCHAR,
    publisher_name VARCHAR,
    total_books INTEGER,
    avg_rating DECIMAL(3,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    publisher_id VARCHAR,
    supplier_name VARCHAR,
    total_products INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key SERIAL PRIMARY KEY,
    category_id VARCHAR,
    level_1 VARCHAR,
    level_2 VARCHAR,
    level_3 VARCHAR,
    full_path VARCHAR,
    parent_category_key INTEGER,
    category_level INTEGER,
    total_products INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR,
    product_name VARCHAR,
    publish_year INTEGER,
    language VARCHAR,
    page_count INTEGER,
    weight DECIMAL(10,2),
    dimensions VARCHAR,
    image_url VARCHAR,
    product_url VARCHAR,
    product_unit VARCHAR,
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,
    version_number INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE,
    day_of_week INTEGER,
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_start_date DATE,
    week_end_date DATE,
    month INTEGER,
    month_name VARCHAR,
    month_year VARCHAR,
    week_of_year INTEGER,
    is_last_day_of_month BOOLEAN,
    is_first_day_of_month BOOLEAN,
    quarter INTEGER,
    quarter_name VARCHAR,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    is_workday BOOLEAN,
    holiday_name VARCHAR
);

-- Fact table
CREATE TABLE IF NOT EXISTS fact_book_sales (
    fact_id SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    author_key INTEGER REFERENCES dim_author(author_key),
    publisher_key INTEGER REFERENCES dim_publisher(publisher_key),
    category_key INTEGER REFERENCES dim_category(category_key),
    supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
    quantity_sold INTEGER,
    review_count INTEGER,
    revenue DECIMAL(15,2),
    original_price DECIMAL(15,2),
    discount_price DECIMAL(15,2),
    discount_percent DECIMAL(5,2),
    rating DECIMAL(3,2),
    is_bestseller BOOLEAN,
    is_on_sale BOOLEAN,
    url_path VARCHAR,
    crawl_timestamp TIMESTAMP,
    etl_batch_id INTEGER,
    data_src VARCHAR,
    created_at TIMESTAMP
);

-- Log tables
CREATE TABLE IF NOT EXISTS crawl_log (
    log_id SERIAL PRIMARY KEY,
    crawl_date DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    category_crawled VARCHAR,
    page_range VARCHAR,
    total_found INTEGER,
    total_inserted INTEGER,
    total_updated INTEGER,
    total_failed INTEGER,
    records_per_second DECIMAL(10,2),
    avg_processing_time DECIMAL(10,2),
    status VARCHAR,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    quality_id SERIAL PRIMARY KEY,
    check_date DATE,
    check_time TIMESTAMP,
    table_name VARCHAR,
    rule_name VARCHAR,
    rule_desc TEXT,
    severity VARCHAR,
    records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    failure_rate DECIMAL(5,2),
    sample_failed_records TEXT,
    action_taken VARCHAR
);
