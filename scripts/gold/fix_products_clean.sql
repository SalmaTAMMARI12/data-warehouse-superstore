-- Fix: Clean populate dim_products without duplicates
TRUNCATE TABLE gold.dim_products RESTART IDENTITY;

INSERT INTO gold.dim_products (product_id, product_name, category, sub_category)
SELECT DISTINCT ON (product_id)
    product_id,
    product_name,
    category,
    sub_category
FROM silver.cleaned_sales
WHERE product_id IS NOT NULL
ORDER BY product_id;

-- Verify counts
SELECT 'Bronze Layer' as layer, COUNT(*) as row_count FROM bronze.raw_sales
UNION ALL
SELECT 'Silver Layer', COUNT(*) FROM silver.cleaned_sales
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM gold.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'dim_locations', COUNT(*) FROM gold.dim_locations
UNION ALL
SELECT 'dim_ship_modes', COUNT(*) FROM gold.dim_ship_modes
UNION ALL
SELECT 'dim_dates', COUNT(*) FROM gold.dim_dates
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM gold.fact_sales;
