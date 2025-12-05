/*
===============================================================================
DDL Script: Create Gold Dimension Tables (FIXED - All as TABLES)
===============================================================================
Script Purpose:
    This script creates ALL dimensions in the Gold layer as TABLES (not views)
    to allow INSERT/UPDATE/UPSERT operations via procedures.

    Each dimension table performs aggregations and transformations from 
    the Silver layer to produce clean, business-ready dimension data.

Usage:
    \i scripts/gold/ddl_gold_tables.sql
===============================================================================
*/

-- Drop all existing Gold objects if they exist
DROP VIEW IF EXISTS gold.fact_sales;
DROP TABLE IF EXISTS gold.dim_dates;
DROP TABLE IF EXISTS gold.dim_ship_modes;
DROP TABLE IF EXISTS gold.dim_locations;
DROP TABLE IF EXISTS gold.dim_products;
DROP TABLE IF EXISTS gold.dim_customers;

-- =============================================================================
-- Create Dimension Table: gold.dim_customers
-- =============================================================================
CREATE TABLE gold.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(255),
    segment VARCHAR(50),
    customer_since_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customers_id ON gold.dim_customers(customer_id);
CREATE INDEX idx_dim_customers_segment ON gold.dim_customers(segment);

-- =============================================================================
-- Create Dimension Table: gold.dim_products
-- =============================================================================
CREATE TABLE gold.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_id ON gold.dim_products(product_id);
CREATE INDEX idx_dim_products_category ON gold.dim_products(category);
CREATE INDEX idx_dim_products_sub_category ON gold.dim_products(sub_category);

-- =============================================================================
-- Create Dimension Table: gold.dim_locations
-- =============================================================================
CREATE TABLE gold.dim_locations (
    location_key SERIAL PRIMARY KEY,
    country VARCHAR(100),
    region VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country, state, city, postal_code)
);

CREATE INDEX idx_dim_locations_country ON gold.dim_locations(country);
CREATE INDEX idx_dim_locations_state ON gold.dim_locations(state);
CREATE INDEX idx_dim_locations_city ON gold.dim_locations(city);

-- =============================================================================
-- Create Dimension Table: gold.dim_ship_modes
-- =============================================================================
CREATE TABLE gold.dim_ship_modes (
    ship_mode_key SERIAL PRIMARY KEY,
    ship_mode VARCHAR(50) UNIQUE NOT NULL,
    delivery_speed VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_ship_modes_mode ON gold.dim_ship_modes(ship_mode);

-- =============================================================================
-- Create Dimension Table: gold.dim_dates (as TABLE)
-- =============================================================================
CREATE TABLE gold.dim_dates (
    date_key INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day INT,
    month INT,
    year INT,
    day_of_week INT,
    day_name VARCHAR(20),
    day_of_year INT,
    week_of_year INT,
    month_name VARCHAR(20),
    quarter INT,
    quarter_name VARCHAR(5),
    is_weekend BOOLEAN,
    is_sunday BOOLEAN,
    is_saturday BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT,
    first_day_of_month DATE,
    last_day_of_month DATE,
    days_in_month INT,
    date_format_dmY VARCHAR(20),
    date_format_iso VARCHAR(20),
    date_format_display VARCHAR(50),
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_dates_full_date ON gold.dim_dates(full_date);
CREATE INDEX idx_dim_dates_year ON gold.dim_dates(year);
CREATE INDEX idx_dim_dates_year_month ON gold.dim_dates(year, month);
CREATE INDEX idx_dim_dates_quarter ON gold.dim_dates(year, quarter);
CREATE INDEX idx_dim_dates_fiscal_year ON gold.dim_dates(fiscal_year);

-- Populate dim_dates with data range from 2011 to 2021
INSERT INTO gold.dim_dates (
    date_key, full_date, day, month, year, day_of_week, day_name, day_of_year,
    week_of_year, month_name, quarter, quarter_name, is_weekend, is_sunday, is_saturday,
    fiscal_year, fiscal_quarter, first_day_of_month, last_day_of_month, days_in_month,
    date_format_dmY, date_format_iso, date_format_display
)
SELECT 
    EXTRACT(YEAR FROM full_date)::INT * 10000 +
    EXTRACT(MONTH FROM full_date)::INT * 100 +
    EXTRACT(DAY FROM full_date)::INT AS date_key,
    
    full_date,
    EXTRACT(DAY FROM full_date)::INT,
    EXTRACT(MONTH FROM full_date)::INT,
    EXTRACT(YEAR FROM full_date)::INT,
    EXTRACT(DOW FROM full_date)::INT,
    TO_CHAR(full_date, 'Day'),
    EXTRACT(DOY FROM full_date)::INT,
    EXTRACT(WEEK FROM full_date)::INT,
    TO_CHAR(full_date, 'Month'),
    EXTRACT(QUARTER FROM full_date)::INT,
    'Q' || EXTRACT(QUARTER FROM full_date)::TEXT,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END,
    CASE WHEN EXTRACT(DOW FROM full_date) = 0 THEN TRUE ELSE FALSE END,
    CASE WHEN EXTRACT(DOW FROM full_date) = 6 THEN TRUE ELSE FALSE END,
    CASE 
        WHEN EXTRACT(MONTH FROM full_date) >= 4 THEN EXTRACT(YEAR FROM full_date)::INT
        ELSE EXTRACT(YEAR FROM full_date)::INT - 1 
    END,
    CASE 
        WHEN EXTRACT(MONTH FROM full_date) IN (4, 5, 6) THEN 1
        WHEN EXTRACT(MONTH FROM full_date) IN (7, 8, 9) THEN 2
        WHEN EXTRACT(MONTH FROM full_date) IN (10, 11, 12) THEN 3
        ELSE 4
    END,
    DATE_TRUNC('month', full_date)::DATE,
    (DATE_TRUNC('month', full_date) + INTERVAL '1 month - 1 day')::DATE,
    EXTRACT(DAY FROM (DATE_TRUNC('month', full_date) + INTERVAL '1 month - 1 day'))::INT,
    TO_CHAR(full_date, 'DD/MM/YYYY'),
    TO_CHAR(full_date, 'YYYY-MM-DD'),
    TO_CHAR(full_date, 'Mon DD, YYYY')
FROM (
    SELECT GENERATE_SERIES('2011-01-01'::DATE, '2021-12-31'::DATE, '1 day')::DATE AS full_date
) d;

-- Mark US holidays
UPDATE gold.dim_dates SET is_holiday = TRUE, holiday_name = 'New Year''s Day' WHERE month = 1 AND day = 1;
UPDATE gold.dim_dates SET is_holiday = TRUE, holiday_name = 'Independence Day' WHERE month = 7 AND day = 4;
UPDATE gold.dim_dates SET is_holiday = TRUE, holiday_name = 'Christmas' WHERE month = 12 AND day = 25;
UPDATE gold.dim_dates SET is_holiday = TRUE, holiday_name = 'Thanksgiving' 
WHERE month = 11 AND day_of_week = 4 AND day BETWEEN 22 AND 28;

-- =============================================================================
-- Populate Dimension Tables from Silver Layer
-- =============================================================================

-- Populate dim_customers
INSERT INTO gold.dim_customers (customer_id, customer_name, segment, customer_since_date)
SELECT 
    customer_id,
    customer_name,
    segment,
    MIN(order_date)
FROM silver.cleaned_sales
WHERE customer_id IS NOT NULL
GROUP BY customer_id, customer_name, segment
ON CONFLICT (customer_id) DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    segment = EXCLUDED.segment,
    customer_since_date = CASE 
        WHEN EXCLUDED.customer_since_date < gold.dim_customers.customer_since_date 
        THEN EXCLUDED.customer_since_date
        ELSE gold.dim_customers.customer_since_date
    END,
    updated_at = CURRENT_TIMESTAMP;

-- Populate dim_products using DISTINCT ON to handle duplicates
INSERT INTO gold.dim_products (product_id, product_name, category, sub_category)
SELECT DISTINCT ON (product_id)
    product_id,
    product_name,
    category,
    sub_category
FROM silver.cleaned_sales
WHERE product_id IS NOT NULL
ORDER BY product_id;

-- Populate dim_locations
INSERT INTO gold.dim_locations (country, region, state, city, postal_code)
SELECT DISTINCT
    country,
    region,
    state,
    city,
    postal_code
FROM silver.cleaned_sales
WHERE country IS NOT NULL
ON CONFLICT (country, state, city, postal_code) DO NOTHING;

-- Populate dim_ship_modes
INSERT INTO gold.dim_ship_modes (ship_mode, delivery_speed)
SELECT DISTINCT
    ship_mode,
    CASE 
        WHEN ship_mode ILIKE '%same day%' THEN 'Express'
        WHEN ship_mode ILIKE '%first class%' THEN 'Fast'
        WHEN ship_mode ILIKE '%second class%' THEN 'Standard'
        ELSE 'Economy'
    END
FROM silver.cleaned_sales
WHERE ship_mode IS NOT NULL
ON CONFLICT (ship_mode) DO UPDATE SET
    delivery_speed = EXCLUDED.delivery_speed,
    updated_at = CURRENT_TIMESTAMP;

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    s.row_id AS sales_line_id,
    s.order_id,

    d_order.date_key AS order_date_key,
    d_ship.date_key AS ship_date_key,
    
    sm.ship_mode_key,
    p.product_key,
    c.customer_key,
    l.location_key,

    s.sales,
    s.quantity,
    s.discount,
    s.profit
FROM silver.cleaned_sales s
LEFT JOIN gold.dim_products p
    ON s.product_id = p.product_id
LEFT JOIN gold.dim_customers c
    ON s.customer_id = c.customer_id
LEFT JOIN gold.dim_ship_modes sm
    ON s.ship_mode = sm.ship_mode
LEFT JOIN gold.dim_locations l
    ON s.country = l.country AND s.state = l.state AND s.city = l.city
LEFT JOIN gold.dim_dates d_order
    ON d_order.date_key = TO_CHAR(s.order_date, 'YYYYMMDD')::INT
LEFT JOIN gold.dim_dates d_ship
    ON d_ship.date_key = TO_CHAR(s.ship_date, 'YYYYMMDD')::INT;
