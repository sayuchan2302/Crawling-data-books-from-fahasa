-- ============================================
-- STAGING CONTROL TABLES
-- ============================================

-- 1. ETL Batch Control Log
CREATE TABLE IF NOT EXISTS staging_control_log (
    batch_id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL DEFAULT CURRENT_DATE,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- RUNNING, SUCCESS, FAILED, PARTIAL
    source_type VARCHAR(50) NOT NULL, -- CRAWLER, CSV_IMPORT, API
    source_identifier TEXT, -- File path, URL, etc
    
    -- Counts
    records_extracted INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    records_duplicate INTEGER DEFAULT 0,
    
    -- Performance metrics
    duration_seconds INTEGER,
    records_per_second DECIMAL(10,2),
    
    -- Error handling
    error_message TEXT,
    error_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_by VARCHAR(100) DEFAULT 'system',
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Data Quality Control
CREATE TABLE IF NOT EXISTS staging_data_quality (
    quality_id SERIAL PRIMARY KEY,
    batch_id INTEGER REFERENCES staging_control_log(batch_id),
    
    -- Check details
    check_name VARCHAR(100) NOT NULL,
    check_description TEXT,
    check_query TEXT, -- SQL query used for validation
    
    -- Results
    total_records INTEGER NOT NULL,
    passed_records INTEGER NOT NULL,
    failed_records INTEGER NOT NULL,
    failure_rate DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE 
            WHEN total_records > 0 
            THEN (failed_records::DECIMAL / total_records * 100)
            ELSE 0 
        END
    ) STORED,
    
    -- Thresholds
    warning_threshold DECIMAL(5,2) DEFAULT 5.0, -- % failure rate
    critical_threshold DECIMAL(5,2) DEFAULT 10.0,
    
    -- Status
    status VARCHAR(20) NOT NULL, -- PASS, WARNING, CRITICAL, FAILED
    
    -- Sample data
    sample_failed_records JSONB, -- Sample of failed records
    
    -- Timing
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Staging Data Validation Rules
CREATE TABLE IF NOT EXISTS staging_validation_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    rule_description TEXT,
    rule_query TEXT NOT NULL, -- SQL query for validation
    rule_type VARCHAR(50) NOT NULL, -- NULL_CHECK, TYPE_CHECK, RANGE_CHECK, BUSINESS_RULE
    
    -- Thresholds
    warning_threshold DECIMAL(5,2) DEFAULT 5.0,
    critical_threshold DECIMAL(5,2) DEFAULT 10.0,
    
    -- Control
    is_active BOOLEAN DEFAULT TRUE,
    is_blocking BOOLEAN DEFAULT FALSE, -- Stop ETL if this fails?
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Enhanced staging_books with control fields
ALTER TABLE staging_books ADD COLUMN IF NOT EXISTS batch_id INTEGER;
ALTER TABLE staging_books ADD COLUMN IF NOT EXISTS load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE staging_books ADD COLUMN IF NOT EXISTS data_quality_score DECIMAL(3,2); -- 0.0 to 1.0
ALTER TABLE staging_books ADD COLUMN IF NOT EXISTS validation_errors JSONB;
ALTER TABLE staging_books ADD COLUMN IF NOT EXISTS record_status VARCHAR(20) DEFAULT 'VALID'; -- VALID, WARNING, ERROR

-- Add foreign key constraint
ALTER TABLE staging_books ADD CONSTRAINT fk_staging_batch 
    FOREIGN KEY (batch_id) REFERENCES staging_control_log(batch_id);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Control log indexes
CREATE INDEX IF NOT EXISTS idx_staging_control_batch_date ON staging_control_log(batch_date);
CREATE INDEX IF NOT EXISTS idx_staging_control_status ON staging_control_log(status);
CREATE INDEX IF NOT EXISTS idx_staging_control_source ON staging_control_log(source_type);

-- Data quality indexes
CREATE INDEX IF NOT EXISTS idx_staging_quality_batch ON staging_data_quality(batch_id);
CREATE INDEX IF NOT EXISTS idx_staging_quality_status ON staging_data_quality(status);
CREATE INDEX IF NOT EXISTS idx_staging_quality_check ON staging_data_quality(check_name);

-- Staging books indexes for control
CREATE INDEX IF NOT EXISTS idx_staging_books_batch ON staging_books(batch_id);
CREATE INDEX IF NOT EXISTS idx_staging_books_load_time ON staging_books(load_timestamp);
CREATE INDEX IF NOT EXISTS idx_staging_books_status ON staging_books(record_status);

-- ============================================
-- INSERT DEFAULT VALIDATION RULES
-- ============================================

INSERT INTO staging_validation_rules (rule_name, rule_description, rule_query, rule_type, warning_threshold, critical_threshold, is_blocking) VALUES
('title_not_null', 'Check if title is not null', 
 'SELECT COUNT(*) as failed FROM staging_books WHERE batch_id = %s AND (title IS NULL OR title = '''')', 
 'NULL_CHECK', 1.0, 5.0, true),

('price_validation', 'Check if prices are valid numbers', 
 'SELECT COUNT(*) as failed FROM staging_books WHERE batch_id = %s AND (original_price < 0 OR discount_price < 0)', 
 'RANGE_CHECK', 2.0, 5.0, false),

('rating_range', 'Check if rating is between 0 and 5', 
 'SELECT COUNT(*) as failed FROM staging_books WHERE batch_id = %s AND (rating < 0 OR rating > 5)', 
 'RANGE_CHECK', 1.0, 3.0, false),

('url_format', 'Check if URL contains fahasa domain', 
 'SELECT COUNT(*) as failed FROM staging_books WHERE batch_id = %s AND (url NOT LIKE ''%fahasa%'' AND url IS NOT NULL)', 
 'BUSINESS_RULE', 5.0, 10.0, false),

('duplicate_detection', 'Check for duplicate URLs in batch', 
 'SELECT COUNT(*) - COUNT(DISTINCT url) as failed FROM staging_books WHERE batch_id = %s AND url IS NOT NULL', 
 'BUSINESS_RULE', 1.0, 2.0, true);

-- ============================================
-- VIEWS FOR MONITORING
-- ============================================

-- ETL Dashboard view
CREATE OR REPLACE VIEW v_staging_etl_dashboard AS
SELECT 
    scl.batch_id,
    scl.batch_date,
    scl.status,
    scl.source_type,
    scl.records_extracted,
    scl.records_loaded,
    scl.records_rejected,
    scl.duration_seconds,
    scl.records_per_second,
    COUNT(sdq.quality_id) as quality_checks_count,
    COUNT(CASE WHEN sdq.status = 'CRITICAL' THEN 1 END) as critical_issues,
    COUNT(CASE WHEN sdq.status = 'WARNING' THEN 1 END) as warning_issues
FROM staging_control_log scl
LEFT JOIN staging_data_quality sdq ON scl.batch_id = sdq.batch_id
GROUP BY scl.batch_id, scl.batch_date, scl.status, scl.source_type, 
         scl.records_extracted, scl.records_loaded, scl.records_rejected,
         scl.duration_seconds, scl.records_per_second
ORDER BY scl.batch_id DESC;

-- Data Quality Summary
CREATE OR REPLACE VIEW v_data_quality_summary AS
SELECT 
    check_name,
    COUNT(*) as total_runs,
    AVG(failure_rate) as avg_failure_rate,
    MAX(failure_rate) as max_failure_rate,
    COUNT(CASE WHEN status = 'CRITICAL' THEN 1 END) as critical_failures,
    COUNT(CASE WHEN status = 'WARNING' THEN 1 END) as warnings,
    COUNT(CASE WHEN status = 'PASS' THEN 1 END) as passes
FROM staging_data_quality 
GROUP BY check_name
ORDER BY avg_failure_rate DESC;

COMMENT ON VIEW v_staging_etl_dashboard IS 'ETL batch monitoring dashboard';
COMMENT ON VIEW v_data_quality_summary IS 'Data quality checks summary across all batches';