-- E-commerce Analytics Warehouse - Snowflake Schema
-- Optimized for customer behavior analysis and product analytics

-- Create database
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE DATABASE ecommerce_analytics;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS agg;
CREATE SCHEMA IF NOT EXISTS meta;

-- =====================================================
-- STAGING TABLES
-- =====================================================

USE SCHEMA staging;

-- Staging: Web events
CREATE OR REPLACE TABLE staging.web_events (
    event_id VARCHAR(50),
    customer_id VARCHAR(50),
    session_id VARCHAR(50),
    event_timestamp TIMESTAMP_NTZ,
    event_type VARCHAR(50),
    page_url VARCHAR(500),
    referrer_url VARCHAR(500),
    device_type VARCHAR(50),
    browser VARCHAR(50),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging: Orders
CREATE OR REPLACE TABLE staging.orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR(50),
    order_total NUMBER(18,2),
    shipping_cost NUMBER(18,2),
    tax_amount NUMBER(18,2),
    discount_amount NUMBER(18,2),
    payment_method VARCHAR(50),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging: Order items
CREATE OR REPLACE TABLE staging.order_items (
    order_item_id VARCHAR(50),
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price NUMBER(18,2),
    discount NUMBER(18,2),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging: Products
CREATE OR REPLACE TABLE staging.products (
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price NUMBER(18,2),
    cost NUMBER(18,2),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging: Customers
CREATE OR REPLACE TABLE staging.customers (
    customer_id VARCHAR(50),
    email VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    registration_date DATE,
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

USE SCHEMA dim;

-- Dimension: Customer (SCD Type 2)
CREATE OR REPLACE TABLE dim.customer (
    customer_key INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    email VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    registration_date DATE,
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    customer_segment VARCHAR(50),
    rfm_score VARCHAR(10),
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Product (SCD Type 2)
CREATE OR REPLACE TABLE dim.product (
    product_key INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price NUMBER(18,2),
    cost NUMBER(18,2),
    margin_pct NUMBER(10,2),
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Date
CREATE OR REPLACE TABLE dim.date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER
);

-- Dimension: Time
CREATE OR REPLACE TABLE dim.time (
    time_key INTEGER PRIMARY KEY,
    hour INTEGER,
    minute INTEGER,
    second INTEGER,
    time_of_day VARCHAR(20),
    is_business_hours BOOLEAN
);

-- Dimension: Session
CREATE OR REPLACE TABLE dim.session (
    session_key INTEGER AUTOINCREMENT PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL,
    customer_key INTEGER,
    session_start_timestamp TIMESTAMP_NTZ,
    session_end_timestamp TIMESTAMP_NTZ,
    session_duration_seconds INTEGER,
    device_type VARCHAR(50),
    browser VARCHAR(50),
    referrer_source VARCHAR(100),
    landing_page VARCHAR(500),
    exit_page VARCHAR(500),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- FACT TABLES
-- =====================================================

USE SCHEMA fact;

-- Fact: Web Events
CREATE OR REPLACE TABLE fact.web_events (
    event_key INTEGER AUTOINCREMENT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    time_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    session_key INTEGER NOT NULL,
    event_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50),
    page_url VARCHAR(500),
    page_views INTEGER DEFAULT 1,
    product_views INTEGER DEFAULT 0,
    add_to_cart INTEGER DEFAULT 0,
    remove_from_cart INTEGER DEFAULT 0,
    checkout_started INTEGER DEFAULT 0,
    purchase_completed INTEGER DEFAULT 0,
    event_timestamp TIMESTAMP_NTZ,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (date_key) REFERENCES dim.date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim.customer(customer_key)
);

-- Fact: Orders
CREATE OR REPLACE TABLE fact.orders (
    order_key INTEGER AUTOINCREMENT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR(50),
    order_total NUMBER(18,2),
    shipping_cost NUMBER(18,2),
    tax_amount NUMBER(18,2),
    discount_amount NUMBER(18,2),
    net_revenue NUMBER(18,2),
    payment_method VARCHAR(50),
    is_first_order BOOLEAN,
    days_since_last_order INTEGER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (date_key) REFERENCES dim.date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim.customer(customer_key)
);

-- Fact: Order Items
CREATE OR REPLACE TABLE fact.order_items (
    order_item_key INTEGER AUTOINCREMENT PRIMARY KEY,
    order_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    order_item_id VARCHAR(50) NOT NULL,
    quantity INTEGER,
    unit_price NUMBER(18,2),
    discount NUMBER(18,2),
    line_total NUMBER(18,2),
    cost NUMBER(18,2),
    profit NUMBER(18,2),
    profit_margin_pct NUMBER(10,2),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (order_key) REFERENCES fact.orders(order_key),
    FOREIGN KEY (product_key) REFERENCES dim.product(product_key)
);

-- Fact: Customer Behavior
CREATE OR REPLACE TABLE fact.customer_behavior (
    behavior_key INTEGER AUTOINCREMENT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    session_key INTEGER NOT NULL,
    total_page_views INTEGER,
    total_product_views INTEGER,
    total_add_to_cart INTEGER,
    total_remove_from_cart INTEGER,
    checkout_started BOOLEAN,
    purchase_completed BOOLEAN,
    cart_abandoned BOOLEAN,
    session_duration_seconds INTEGER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (date_key) REFERENCES dim.date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim.customer(customer_key),
    FOREIGN KEY (session_key) REFERENCES dim.session(session_key)
);

-- =====================================================
-- AGGREGATE TABLES
-- =====================================================

USE SCHEMA agg;

-- Aggregate: Daily metrics
CREATE OR REPLACE TABLE agg.daily_metrics (
    metric_date DATE PRIMARY KEY,
    total_sessions INTEGER,
    unique_visitors INTEGER,
    total_page_views INTEGER,
    total_orders INTEGER,
    total_revenue NUMBER(18,2),
    avg_order_value NUMBER(18,2),
    conversion_rate NUMBER(10,4),
    cart_abandonment_rate NUMBER(10,4),
    new_customers INTEGER,
    returning_customers INTEGER,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Aggregate: Product performance
CREATE OR REPLACE TABLE agg.product_performance (
    product_key INTEGER,
    metric_date DATE,
    views INTEGER,
    add_to_cart INTEGER,
    purchases INTEGER,
    quantity_sold INTEGER,
    revenue NUMBER(18,2),
    profit NUMBER(18,2),
    conversion_rate NUMBER(10,4),
    PRIMARY KEY (product_key, metric_date),
    FOREIGN KEY (product_key) REFERENCES dim.product(product_key)
);

-- Aggregate: Customer RFM scores
CREATE OR REPLACE TABLE agg.customer_rfm (
    customer_key INTEGER PRIMARY KEY,
    recency_days INTEGER,
    frequency INTEGER,
    monetary NUMBER(18,2),
    r_score INTEGER,
    f_score INTEGER,
    m_score INTEGER,
    rfm_score VARCHAR(10),
    customer_segment VARCHAR(50),
    last_order_date DATE,
    first_order_date DATE,
    total_orders INTEGER,
    total_revenue NUMBER(18,2),
    avg_order_value NUMBER(18,2),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_key) REFERENCES dim.customer(customer_key)
);

-- Aggregate: Conversion funnel
CREATE OR REPLACE TABLE agg.conversion_funnel (
    funnel_date DATE,
    stage VARCHAR(50),
    stage_order INTEGER,
    sessions INTEGER,
    conversion_rate NUMBER(10,4),
    drop_off_rate NUMBER(10,4),
    PRIMARY KEY (funnel_date, stage)
);

-- Aggregate: Cohort retention
CREATE OR REPLACE TABLE agg.cohort_retention (
    cohort_month DATE,
    months_since_first INTEGER,
    customers INTEGER,
    retention_rate NUMBER(10,4),
    revenue NUMBER(18,2),
    PRIMARY KEY (cohort_month, months_since_first)
);

-- Aggregate: Product affinity
CREATE OR REPLACE TABLE agg.product_affinity (
    product_a_key INTEGER,
    product_b_key INTEGER,
    co_occurrence_count INTEGER,
    support NUMBER(10,6),
    confidence NUMBER(10,6),
    lift NUMBER(10,4),
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (product_a_key, product_b_key),
    FOREIGN KEY (product_a_key) REFERENCES dim.product(product_key),
    FOREIGN KEY (product_b_key) REFERENCES dim.product(product_key)
);

-- =====================================================
-- METADATA AND AUDIT
-- =====================================================

USE SCHEMA meta;

-- ETL audit log
CREATE OR REPLACE TABLE meta.etl_audit_log (
    audit_id INTEGER AUTOINCREMENT PRIMARY KEY,
    pipeline_name VARCHAR(100),
    start_timestamp TIMESTAMP_NTZ,
    end_timestamp TIMESTAMP_NTZ,
    status VARCHAR(20),
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_failed INTEGER,
    error_message VARCHAR(5000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Data quality checks
CREATE OR REPLACE TABLE meta.data_quality_checks (
    check_id INTEGER AUTOINCREMENT PRIMARY KEY,
    check_date DATE,
    table_name VARCHAR(100),
    check_type VARCHAR(50),
    check_result VARCHAR(20),
    records_checked INTEGER,
    records_failed INTEGER,
    details VARCHAR(5000),
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- POPULATE DATE DIMENSION
-- =====================================================

USE SCHEMA dim;

-- Generate dates for 10 years (2020-2030)
INSERT INTO dim.date
WITH date_range AS (
    SELECT 
        DATEADD(day, SEQ4(), '2020-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    WEEKOFYEAR(full_date) AS week,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    DAYNAME(full_date) AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    YEAR(full_date) AS fiscal_year,
    QUARTER(full_date) AS fiscal_quarter,
    MONTH(full_date) AS fiscal_month
FROM date_range;

-- =====================================================
-- POPULATE TIME DIMENSION
-- =====================================================

INSERT INTO dim.time
WITH time_range AS (
    SELECT SEQ4() AS seconds
    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
)
SELECT
    seconds AS time_key,
    FLOOR(seconds / 3600) AS hour,
    FLOOR((seconds % 3600) / 60) AS minute,
    seconds % 60 AS second,
    CASE
        WHEN FLOOR(seconds / 3600) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN FLOOR(seconds / 3600) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN FLOOR(seconds / 3600) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    CASE
        WHEN FLOOR(seconds / 3600) BETWEEN 9 AND 17 THEN TRUE
        ELSE FALSE
    END AS is_business_hours
FROM time_range;

-- =====================================================
-- CREATE VIEWS FOR REPORTING
-- =====================================================

USE SCHEMA agg;

-- View: Customer lifetime value
CREATE OR REPLACE VIEW agg.customer_lifetime_value AS
SELECT
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.customer_segment,
    rfm.total_orders,
    rfm.total_revenue AS lifetime_value,
    rfm.avg_order_value,
    DATEDIFF(day, rfm.first_order_date, rfm.last_order_date) AS customer_age_days,
    rfm.recency_days,
    CASE
        WHEN rfm.recency_days <= 30 THEN 'Active'
        WHEN rfm.recency_days <= 90 THEN 'At Risk'
        ELSE 'Inactive'
    END AS activity_status
FROM dim.customer c
JOIN agg.customer_rfm rfm ON c.customer_key = rfm.customer_key
WHERE c.is_current = TRUE;

-- View: Product bestsellers
CREATE OR REPLACE VIEW agg.product_bestsellers AS
SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    SUM(pp.quantity_sold) AS total_quantity_sold,
    SUM(pp.revenue) AS total_revenue,
    SUM(pp.profit) AS total_profit,
    AVG(pp.conversion_rate) AS avg_conversion_rate
FROM dim.product p
JOIN agg.product_performance pp ON p.product_key = pp.product_key
WHERE p.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_revenue DESC;

-- =====================================================
-- CREATE STREAMS FOR REAL-TIME PROCESSING
-- =====================================================

USE SCHEMA staging;

CREATE OR REPLACE STREAM staging.web_events_stream ON TABLE staging.web_events;
CREATE OR REPLACE STREAM staging.orders_stream ON TABLE staging.orders;

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================

-- Grant usage on database and schemas
GRANT USAGE ON DATABASE ecommerce_analytics TO ROLE analyst_role;
GRANT USAGE ON SCHEMA dim TO ROLE analyst_role;
GRANT USAGE ON SCHEMA fact TO ROLE analyst_role;
GRANT USAGE ON SCHEMA agg TO ROLE analyst_role;

-- Grant select on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA dim TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA fact TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA agg TO ROLE analyst_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA agg TO ROLE analyst_role;

-- =====================================================
-- COMMENTS
-- =====================================================

COMMENT ON DATABASE ecommerce_analytics IS 'E-commerce analytics data warehouse';
COMMENT ON SCHEMA dim IS 'Dimension tables for e-commerce analytics';
COMMENT ON SCHEMA fact IS 'Fact tables for transactional data';
COMMENT ON SCHEMA agg IS 'Aggregated metrics and KPIs';

SELECT 'E-commerce Analytics Warehouse schema created successfully' AS status;
