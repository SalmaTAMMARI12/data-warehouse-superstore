/*
===============================================================================
Quality Checks: Silver Layer (cleaned_sales)
===============================================================================
Purpose:
    Validate data quality in the Silver layer after transformation from Bronze.
    Checks for:
    - NULL values in critical columns
    - Data type conversions
    - Date ranges
    - Outliers and anomalies
    - Duplicate records
===============================================================================
*/

-- =============================================================================
-- 1. RECORD COUNT CHECKS
-- =============================================================================
SELECT 
    'Total Records in silver.cleaned_sales' as check_name,
    COUNT(*) as record_count
FROM silver.cleaned_sales;

SELECT 
    'Records with NULL row_id' as check_name,
    COUNT(*) as null_count
FROM silver.cleaned_sales
WHERE row_id IS NULL;

SELECT 
    'Records with NULL order_date' as check_name,
    COUNT(*) as null_count
FROM silver.cleaned_sales
WHERE order_date IS NULL;

SELECT 
    'Records with NULL customer_id' as check_name,
    COUNT(*) as null_count
FROM silver.cleaned_sales
WHERE customer_id IS NULL;

SELECT 
    'Records with NULL product_id' as check_name,
    COUNT(*) as null_count
FROM silver.cleaned_sales
WHERE product_id IS NULL;

-- =============================================================================
-- 2. DATE VALIDATION
-- =============================================================================
SELECT 
    'Order Dates - Min/Max Range' as check_name,
    MIN(order_date)::TEXT as min_date,
    MAX(order_date)::TEXT as max_date,
    COUNT(DISTINCT order_date) as unique_dates
FROM silver.cleaned_sales
WHERE order_date IS NOT NULL;

SELECT 
    'Ship Dates - Min/Max Range' as check_name,
    MIN(ship_date)::TEXT as min_date,
    MAX(ship_date)::TEXT as max_date,
    COUNT(DISTINCT ship_date) as unique_dates
FROM silver.cleaned_sales
WHERE ship_date IS NOT NULL;

-- Check for invalid dates (ship_date before order_date)
SELECT 
    'Invalid Dates: Ship Date before Order Date' as check_name,
    COUNT(*) as issue_count
FROM silver.cleaned_sales
WHERE ship_date < order_date;

-- =============================================================================
-- 3. NUMERIC VALUE VALIDATION
-- =============================================================================
SELECT 
    'Sales Values - Statistics' as check_name,
    COUNT(*) as total_count,
    COUNT(sales) as non_null_sales,
    MIN(sales)::NUMERIC(15,2) as min_sales,
    MAX(sales)::NUMERIC(15,2) as max_sales,
    AVG(sales)::NUMERIC(15,2) as avg_sales,
    STDDEV(sales)::NUMERIC(15,2) as stddev_sales
FROM silver.cleaned_sales
WHERE sales IS NOT NULL;

SELECT 
    'Negative Sales Values' as check_name,
    COUNT(*) as negative_sales_count
FROM silver.cleaned_sales
WHERE sales < 0;

SELECT 
    'Quantity Values - Statistics' as check_name,
    MIN(quantity) as min_qty,
    MAX(quantity) as max_qty,
    AVG(quantity) as avg_qty
FROM silver.cleaned_sales
WHERE quantity IS NOT NULL;

SELECT 
    'Negative or Zero Quantity' as check_name,
    COUNT(*) as issue_count
FROM silver.cleaned_sales
WHERE quantity <= 0;

SELECT 
    'Discount Values - Range Check' as check_name,
    MIN(discount) as min_discount,
    MAX(discount) as max_discount,
    COUNT(*) as records_with_discount
FROM silver.cleaned_sales
WHERE discount IS NOT NULL AND discount > 0;

-- =============================================================================
-- 4. CATEGORICAL DATA VALIDATION
-- =============================================================================
SELECT 
    'Unique Ship Modes' as check_name,
    COUNT(DISTINCT ship_mode) as unique_count,
    STRING_AGG(DISTINCT ship_mode, ', ' ORDER BY ship_mode) as values
FROM silver.cleaned_sales;

SELECT 
    'Unique Segments' as check_name,
    COUNT(DISTINCT segment) as unique_count,
    STRING_AGG(DISTINCT segment, ', ' ORDER BY segment) as values
FROM silver.cleaned_sales;

SELECT 
    'Unique Regions' as check_name,
    COUNT(DISTINCT region) as unique_count,
    STRING_AGG(DISTINCT region, ', ' ORDER BY region) as values
FROM silver.cleaned_sales;

SELECT 
    'Unique Categories' as check_name,
    COUNT(DISTINCT category) as unique_count,
    STRING_AGG(DISTINCT category, ', ' ORDER BY category) as values
FROM silver.cleaned_sales;

-- =============================================================================
-- 5. DUPLICATE AND UNIQUENESS CHECKS
-- =============================================================================
SELECT 
    'Duplicate row_ids' as check_name,
    COUNT(*) as duplicate_count
FROM (
    SELECT row_id, COUNT(*) as cnt
    FROM silver.cleaned_sales
    GROUP BY row_id
    HAVING COUNT(*) > 1
) dups;

SELECT 
    'Duplicate order_ids' as check_name,
    COUNT(*) as duplicate_count
FROM (
    SELECT order_id, COUNT(*) as cnt
    FROM silver.cleaned_sales
    WHERE order_id IS NOT NULL
    GROUP BY order_id
    HAVING COUNT(*) > 1
) dups;

-- =============================================================================
-- 6. COMPLETENESS CHECKS
-- =============================================================================
SELECT 
    'Data Completeness Summary' as check_name,
    ROUND(100.0 * COUNT(*) FILTER (WHERE order_id IS NOT NULL) / COUNT(*), 2) as pct_order_id,
    ROUND(100.0 * COUNT(*) FILTER (WHERE customer_id IS NOT NULL) / COUNT(*), 2) as pct_customer_id,
    ROUND(100.0 * COUNT(*) FILTER (WHERE product_id IS NOT NULL) / COUNT(*), 2) as pct_product_id,
    ROUND(100.0 * COUNT(*) FILTER (WHERE sales IS NOT NULL) / COUNT(*), 2) as pct_sales,
    ROUND(100.0 * COUNT(*) FILTER (WHERE quantity IS NOT NULL) / COUNT(*), 2) as pct_quantity
FROM silver.cleaned_sales;

-- =============================================================================
-- 7. SUMMARY REPORT
-- =============================================================================
SELECT 
    'SILVER LAYER - DATA QUALITY SUMMARY' as report_title,
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(DISTINCT order_id) as unique_orders,
    COUNT(DISTINCT DATE(order_date)) as unique_order_dates
FROM silver.cleaned_sales;
