/*
===============================================================================
Stored Procedure: Refresh Gold Layer Dimensions
===============================================================================
Description:
    Refreshes all dimension tables in the Gold layer by inserting new records
    and updating existing ones from the Silver layer.
    
    Uses ON CONFLICT to handle updates (UPSERT pattern).
    
Dimensions Refreshed:
    - gold.dim_customers
    - gold.dim_products
    - gold.dim_locations
    - gold.dim_ship_modes
    - gold.dim_dates (usually static, but can be regenerated if needed)

Parameters:
    None

Usage:
    CALL gold.refresh_dimensions();
    
Notes:
    - fact_sales is a VIEW, so it's automatically up-to-date
    - Surrogate keys (SERIAL) remain stable across refreshes
    - Only new records get new surrogate keys
===============================================================================
*/

CREATE OR REPLACE PROCEDURE gold.refresh_dimensions()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
    v_rows_affected INT;
BEGIN
    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Refreshing Gold Layer Dimensions';
    RAISE NOTICE '========================================================';

    -- =========================================================================
    -- 1. Refresh dim_customers
    -- =========================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Refreshing: gold.dim_customers';
    
    INSERT INTO gold.dim_customers (customer_id, customer_name, segment, customer_since_date)
    SELECT 
        customer_id,
        customer_name,
        segment,
        MIN(order_date) AS customer_since_date
    FROM silver.cleaned_sales
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, customer_name, segment
    ON CONFLICT (customer_id) 
    DO UPDATE SET
        customer_name = EXCLUDED.customer_name,
        segment = EXCLUDED.segment,
        customer_since_date = CASE 
            WHEN EXCLUDED.customer_since_date < gold.dim_customers.customer_since_date 
            THEN EXCLUDED.customer_since_date
            ELSE gold.dim_customers.customer_since_date
        END,
        updated_at = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    end_time := clock_timestamp();
    RAISE NOTICE '   - Rows inserted/updated: %', v_rows_affected;
    RAISE NOTICE '   - Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- =========================================================================
    -- 2. Refresh dim_products (using DISTINCT ON to handle duplicates)
    -- =========================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Refreshing: gold.dim_products';
    
    DELETE FROM gold.dim_products;
    
    INSERT INTO gold.dim_products (product_id, product_name, category, sub_category)
    SELECT DISTINCT ON (product_id)
        product_id,
        product_name,
        category,
        sub_category
    FROM silver.cleaned_sales
    WHERE product_id IS NOT NULL
    ORDER BY product_id;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    end_time := clock_timestamp();
    RAISE NOTICE '   - Rows inserted: %', v_rows_affected;
    RAISE NOTICE '   - Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- =========================================================================
    -- 3. Refresh dim_locations
    -- =========================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Refreshing: gold.dim_locations';
    
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
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    end_time := clock_timestamp();
    RAISE NOTICE '   - Rows inserted: %', v_rows_affected;
    RAISE NOTICE '   - Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- =========================================================================
    -- 4. Refresh dim_ship_modes
    -- =========================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Refreshing: gold.dim_ship_modes';
    
    INSERT INTO gold.dim_ship_modes (ship_mode, delivery_speed)
    SELECT DISTINCT
        ship_mode,
        CASE 
            WHEN ship_mode ILIKE '%same day%' THEN 'Express'
            WHEN ship_mode ILIKE '%first class%' THEN 'Fast'
            WHEN ship_mode ILIKE '%second class%' THEN 'Standard'
            ELSE 'Economy'
        END AS delivery_speed
    FROM silver.cleaned_sales
    WHERE ship_mode IS NOT NULL
    ON CONFLICT (ship_mode) 
    DO UPDATE SET
        delivery_speed = EXCLUDED.delivery_speed,
        updated_at = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    end_time := clock_timestamp();
    RAISE NOTICE '   - Rows inserted/updated: %', v_rows_affected;
    RAISE NOTICE '   - Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- =========================================================================
    -- 5. Statistics
    -- =========================================================================
    RAISE NOTICE '';
    RAISE NOTICE '>> Final Dimension Statistics:';
    RAISE NOTICE '   - dim_customers:  % rows', (SELECT COUNT(*) FROM gold.dim_customers);
    RAISE NOTICE '   - dim_products:   % rows', (SELECT COUNT(*) FROM gold.dim_products);
    RAISE NOTICE '   - dim_locations:  % rows', (SELECT COUNT(*) FROM gold.dim_locations);
    RAISE NOTICE '   - dim_ship_modes: % rows', (SELECT COUNT(*) FROM gold.dim_ship_modes);
    RAISE NOTICE '   - dim_dates:      % rows', (SELECT COUNT(*) FROM gold.dim_dates);
    RAISE NOTICE '   - fact_sales:     % rows', (SELECT COUNT(*) FROM gold.fact_sales);

    batch_end_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Gold Layer Dimensions Refreshed Successfully!';
    RAISE NOTICE '   - Total Duration: % seconds', 
                 EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '========================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================================';
        RAISE NOTICE 'ERROR OCCURRED DURING GOLD LAYER REFRESH';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '========================================================';
        RAISE;
END;
$$;


-- =============================================================================
-- Additional Helper Procedures
-- =============================================================================

/*
-------------------------------------------------------------------------------
Procedure: Validate Gold Layer Data Quality
-------------------------------------------------------------------------------
*/
CREATE OR REPLACE PROCEDURE gold.validate_data_quality()
LANGUAGE plpgsql
AS $$
DECLARE
    v_orphaned_customers INT;
    v_orphaned_products INT;
    v_orphaned_dates INT;
    v_null_keys INT;
BEGIN
    RAISE NOTICE '========================================================';
    RAISE NOTICE 'Validating Gold Layer Data Quality';
    RAISE NOTICE '========================================================';

    -- Check for orphaned customers
    SELECT COUNT(*) INTO v_orphaned_customers
    FROM silver.cleaned_sales s
    LEFT JOIN gold.dim_customers c ON s.customer_id = c.customer_id
    WHERE c.customer_key IS NULL AND s.customer_id IS NOT NULL;
    
    RAISE NOTICE '>> Orphaned Customers: %', v_orphaned_customers;

    -- Check for orphaned products
    SELECT COUNT(*) INTO v_orphaned_products
    FROM silver.cleaned_sales s
    LEFT JOIN gold.dim_products p ON s.product_id = p.product_id
    WHERE p.product_key IS NULL AND s.product_id IS NOT NULL;
    
    RAISE NOTICE '>> Orphaned Products: %', v_orphaned_products;

    -- Check for orphaned dates
    SELECT COUNT(*) INTO v_orphaned_dates
    FROM silver.cleaned_sales s
    LEFT JOIN gold.dim_dates d ON TO_CHAR(s.order_date, 'YYYYMMDD')::INT = d.date_key
    WHERE d.date_key IS NULL AND s.order_date IS NOT NULL;
    
    RAISE NOTICE '>> Orphaned Order Dates: %', v_orphaned_dates;

    -- Check for NULL surrogate keys in fact_sales
    SELECT COUNT(*) INTO v_null_keys
    FROM gold.fact_sales
    WHERE customer_key IS NULL 
       OR product_key IS NULL 
       OR location_key IS NULL 
       OR ship_mode_key IS NULL;
    
    RAISE NOTICE '>> Records with NULL surrogate keys: %', v_null_keys;

    IF v_orphaned_customers = 0 AND v_orphaned_products = 0 
       AND v_orphaned_dates = 0 AND v_null_keys = 0 THEN
        RAISE NOTICE '';
        RAISE NOTICE '✓ All validation checks passed!';
    ELSE
        RAISE WARNING 'Some validation checks failed. Review the numbers above.';
    END IF;

    RAISE NOTICE '========================================================';
END;
$$;


/*
-------------------------------------------------------------------------------
Procedure: Full ETL Pipeline (Bronze → Silver → Gold)
-------------------------------------------------------------------------------
*/
CREATE OR REPLACE PROCEDURE etl.run_full_pipeline()
LANGUAGE plpgsql
AS $$
DECLARE
    pipeline_start_time TIMESTAMP := clock_timestamp();
    pipeline_end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '========================================================';
    RAISE NOTICE 'STARTING FULL ETL PIPELINE';
    RAISE NOTICE '========================================================';
    RAISE NOTICE '';

    -- Step 1: Load Bronze
    RAISE NOTICE 'STEP 1/3: Loading Bronze Layer...';
    CALL bronze.load_raw_sales();
    RAISE NOTICE '';

    -- Step 2: Load Silver
    RAISE NOTICE 'STEP 2/3: Loading Silver Layer...';
    CALL silver.load_silver();
    RAISE NOTICE '';

    -- Step 3: Refresh Gold
    RAISE NOTICE 'STEP 3/3: Refreshing Gold Layer...';
    CALL gold.refresh_dimensions();
    RAISE NOTICE '';

    -- Validation
    RAISE NOTICE 'VALIDATION: Checking Data Quality...';
    CALL gold.validate_data_quality();

    pipeline_end_time := clock_timestamp();
    RAISE NOTICE '';
    RAISE NOTICE '========================================================';
    RAISE NOTICE 'FULL ETL PIPELINE COMPLETED SUCCESSFULLY!';
    RAISE NOTICE '   - Total Pipeline Duration: % seconds', 
                 EXTRACT(SECOND FROM pipeline_end_time - pipeline_start_time);
    RAISE NOTICE '========================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '';
        RAISE NOTICE '========================================================';
        RAISE NOTICE 'ETL PIPELINE FAILED';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '========================================================';
        RAISE;
END;
$$;


-- =============================================================================
-- Usage Examples
-- =============================================================================

/*
-- Refresh only Gold dimensions
CALL gold.refresh_dimensions();

-- Validate data quality
CALL gold.validate_data_quality();

-- Run full ETL pipeline (Bronze → Silver → Gold)
CALL etl.run_full_pipeline();

-- Sample analytics query
SELECT 
    c.segment,
    p.category,
    d.year,
    d.quarter_name,
    COUNT(*) AS order_count,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    AVG(f.sales) AS avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
JOIN gold.dim_products p ON f.product_key = p.product_key
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
GROUP BY c.segment, p.category, d.year, d.quarter, d.quarter_name
ORDER BY d.year, d.quarter, total_sales DESC;
*/
