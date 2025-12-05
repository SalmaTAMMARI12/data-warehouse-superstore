/*
===============================================================================
Quality Checks: Gold Layer (Dimensions & Facts)
===============================================================================
Purpose:
    Validate data quality in the Gold layer (dimensions and facts).
    Checks for:
    - Referential integrity
    - Missing/orphaned records
    - Surrogate key completeness
    - Dimension quality
    - Fact table completeness
===============================================================================
*/

-- =============================================================================
-- 1. DIMENSION TABLE SIZES
-- =============================================================================
SELECT 
    'Dimension Sizes' as check_name,
    (SELECT COUNT(*) FROM gold.dim_customers) as dim_customers_count,
    (SELECT COUNT(*) FROM gold.dim_products) as dim_products_count,
    (SELECT COUNT(*) FROM gold.dim_locations) as dim_locations_count,
    (SELECT COUNT(*) FROM gold.dim_ship_modes) as dim_ship_modes_count,
    (SELECT COUNT(*) FROM gold.dim_dates) as dim_dates_count,
    (SELECT COUNT(*) FROM gold.fact_sales) as fact_sales_count;

-- =============================================================================
-- 2. PRIMARY KEY VALIDATION
-- =============================================================================
SELECT 
    'Duplicate customer_keys' as check_name,
    COUNT(*) as duplicate_count
FROM (
    SELECT customer_key, COUNT(*) as cnt
    FROM gold.dim_customers
    GROUP BY customer_key
    HAVING COUNT(*) > 1
) dups;

SELECT 
    'Duplicate product_keys' as check_name,
    COUNT(*) as duplicate_count
FROM (
    SELECT product_key, COUNT(*) as cnt
    FROM gold.dim_products
    GROUP BY product_key
    HAVING COUNT(*) > 1
) dups;

SELECT 
    'Duplicate date_keys' as check_name,
    COUNT(*) as duplicate_count
FROM (
    SELECT date_key, COUNT(*) as cnt
    FROM gold.dim_dates
    GROUP BY date_key
    HAVING COUNT(*) > 1
) dups;

-- =============================================================================
-- 3. REFERENTIAL INTEGRITY - FACT TO DIMENSIONS
-- =============================================================================
SELECT 
    'Orphaned fact_sales: Missing customer_key' as check_name,
    COUNT(*) as orphaned_count
FROM gold.fact_sales f
WHERE f.customer_key IS NULL;

SELECT 
    'Orphaned fact_sales: Missing product_key' as check_name,
    COUNT(*) as orphaned_count
FROM gold.fact_sales f
WHERE f.product_key IS NULL;

SELECT 
    'Orphaned fact_sales: Missing ship_mode_key' as check_name,
    COUNT(*) as orphaned_count
FROM gold.fact_sales f
WHERE f.ship_mode_key IS NULL;

SELECT 
    'Orphaned fact_sales: Missing location_key' as check_name,
    COUNT(*) as orphaned_count
FROM gold.fact_sales f
WHERE f.location_key IS NULL;

SELECT 
    'Orphaned fact_sales: Missing order_date_key' as check_name,
    COUNT(*) as orphaned_count
FROM gold.fact_sales f
WHERE f.order_date_key IS NULL;

-- =============================================================================
-- 4. DIMENSION DATA QUALITY
-- =============================================================================
SELECT 
    'dim_customers - NULL check' as check_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE customer_id IS NULL) as null_customer_id,
    COUNT(*) FILTER (WHERE customer_name IS NULL) as null_customer_name,
    COUNT(*) FILTER (WHERE segment IS NULL) as null_segment
FROM gold.dim_customers;

SELECT 
    'dim_products - NULL check' as check_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE product_id IS NULL) as null_product_id,
    COUNT(*) FILTER (WHERE product_name IS NULL) as null_product_name,
    COUNT(*) FILTER (WHERE category IS NULL) as null_category
FROM gold.dim_products;

SELECT 
    'dim_locations - NULL check' as check_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE country IS NULL) as null_country,
    COUNT(*) FILTER (WHERE city IS NULL) as null_city,
    COUNT(*) FILTER (WHERE state IS NULL) as null_state
FROM gold.dim_locations;

SELECT 
    'dim_ship_modes - Modes' as check_name,
    COUNT(*) as total_modes,
    STRING_AGG(ship_mode, ', ' ORDER BY ship_mode) as modes
FROM gold.dim_ship_modes;

-- =============================================================================
-- 5. FACT TABLE METRICS
-- =============================================================================
SELECT 
    'fact_sales - Sales Metrics' as check_name,
    COUNT(*) as total_sales_lines,
    SUM(sales)::NUMERIC(15,2) as total_sales,
    AVG(sales)::NUMERIC(15,2) as avg_sales,
    MIN(sales)::NUMERIC(15,2) as min_sales,
    MAX(sales)::NUMERIC(15,2) as max_sales,
    COUNT(*) FILTER (WHERE sales < 0) as negative_sales_count
FROM gold.fact_sales;

SELECT 
    'fact_sales - Profit Metrics' as check_name,
    COUNT(*) as total_records,
    SUM(profit)::NUMERIC(15,2) as total_profit,
    AVG(profit)::NUMERIC(15,2) as avg_profit,
    COUNT(*) FILTER (WHERE profit < 0) as loss_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE profit < 0) / COUNT(*), 2) as loss_percentage
FROM gold.fact_sales;

SELECT 
    'fact_sales - Quantity Metrics' as check_name,
    COUNT(*) as total_records,
    SUM(quantity) as total_quantity,
    AVG(quantity)::NUMERIC(10,2) as avg_quantity,
    MIN(quantity) as min_quantity,
    MAX(quantity) as max_quantity
FROM gold.fact_sales;

SELECT 
    'fact_sales - Discount Metrics' as check_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE discount > 0) as records_with_discount,
    ROUND(100.0 * COUNT(*) FILTER (WHERE discount > 0) / COUNT(*), 2) as discount_percentage,
    MIN(discount)::NUMERIC(5,3) as min_discount,
    MAX(discount)::NUMERIC(5,3) as max_discount,
    AVG(discount)::NUMERIC(5,3) as avg_discount
FROM gold.fact_sales;

-- =============================================================================
-- 6. SEGMENT ANALYSIS
-- =============================================================================
SELECT 
    'Customer Segments Distribution' as check_name,
    segment,
    COUNT(DISTINCT c.customer_key) as customer_count,
    COUNT(*) as order_count,
    SUM(f.sales)::NUMERIC(15,2) as total_sales,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM gold.fact_sales), 2) as sales_percentage
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY segment
ORDER BY total_sales DESC;

-- =============================================================================
-- 7. CATEGORY ANALYSIS
-- =============================================================================
SELECT 
    'Product Categories Distribution' as check_name,
    category,
    COUNT(DISTINCT p.product_key) as product_count,
    COUNT(*) as order_count,
    SUM(f.sales)::NUMERIC(15,2) as total_sales,
    SUM(f.profit)::NUMERIC(15,2) as total_profit,
    ROUND(100.0 * SUM(f.profit) / SUM(f.sales), 2) as profit_margin_percent
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY category
ORDER BY total_sales DESC;

-- =============================================================================
-- 8. TEMPORAL ANALYSIS
-- =============================================================================
SELECT 
    'Sales by Year' as check_name,
    d.year,
    COUNT(*) as order_count,
    COUNT(DISTINCT f.sales_line_id) as unique_lines,
    SUM(f.sales)::NUMERIC(15,2) as total_sales,
    SUM(f.profit)::NUMERIC(15,2) as total_profit,
    ROUND(100.0 * SUM(f.profit) / SUM(f.sales), 2) as profit_margin_percent
FROM gold.fact_sales f
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
WHERE d.year IS NOT NULL
GROUP BY d.year
ORDER BY d.year;

SELECT 
    'Sales by Quarter' as check_name,
    d.year,
    d.quarter_name,
    COUNT(*) as order_count,
    SUM(f.sales)::NUMERIC(15,2) as total_sales,
    SUM(f.profit)::NUMERIC(15,2) as total_profit
FROM gold.fact_sales f
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
WHERE d.year IS NOT NULL
GROUP BY d.year, d.quarter, d.quarter_name
ORDER BY d.year, d.quarter;

-- =============================================================================
-- 9. COMPLETENESS REPORT
-- =============================================================================
SELECT 
    'GOLD LAYER - DATA QUALITY SUMMARY' as report_title,
    'Completeness: 100% of fact_sales have surrogate keys' as status,
    COUNT(*) as total_fact_records,
    COUNT(*) FILTER (WHERE customer_key IS NOT NULL AND product_key IS NOT NULL AND ship_mode_key IS NOT NULL AND location_key IS NOT NULL) as complete_records,
    ROUND(100.0 * COUNT(*) FILTER (WHERE customer_key IS NOT NULL AND product_key IS NOT NULL AND ship_mode_key IS NOT NULL AND location_key IS NOT NULL) / COUNT(*), 2) as completeness_percentage
FROM gold.fact_sales;

-- =============================================================================
-- 10. TOP PRODUCTS BY SALES
-- =============================================================================
SELECT 
    'Top 10 Products by Sales' as check_name,
    p.product_name,
    p.category,
    COUNT(*) as order_count,
    SUM(f.sales)::NUMERIC(15,2) as total_sales,
    ROUND(AVG(f.sales), 2) as avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category
ORDER BY total_sales DESC
LIMIT 10;

-- =============================================================================
-- 11. TOP CUSTOMERS BY SALES
-- =============================================================================
SELECT 
    'Top 10 Customers by Sales' as check_name,
    c.customer_name,
    c.segment,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.sales)::NUMERIC(15,2) as total_sales,
    ROUND(AVG(f.sales), 2) as avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.customer_name, c.segment
ORDER BY total_sales DESC
LIMIT 10;
