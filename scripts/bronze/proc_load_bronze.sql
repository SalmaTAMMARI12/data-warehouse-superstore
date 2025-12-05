/*
===============================================================================
Stored Procedure: Load Bronze Layer from Superstore CSV
===============================================================================
Script Purpose:
    Loads data from the Superstore dataset CSV into bronze.raw_sales table.
    - Truncates the table before loading data.
    - Uses the `COPY` command to load data.

Parameters:
    None.
Usage Example:
    CALL bronze.load_raw_sales();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_raw_sales()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
    batch_end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer - Raw Sales';
    RAISE NOTICE '================================================';

    -- bronze.raw_sales
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.raw_sales';
    TRUNCATE TABLE bronze.raw_sales;

    RAISE NOTICE '>> Inserting Data Into: bronze.raw_sales';
    COPY bronze.raw_sales(
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment, country, city,
    state, postal_code, region, product_id, category,
    sub_category, product_name, sales, quantity, discount, profit
)
    FROM 'C:/Users/salma/Desktop/mes projet/DW/data-warehouse-sql-retail-sales/datasets/Superstore.utf8.csv'
    DELIMITER ',' 
    CSV HEADER
    ENCODING 'UTF8';
    UPDATE bronze.raw_sales
    SET source_file = 'Superstore.csv'
    WHERE source_file IS NULL;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer - Raw Sales Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING RAW SALES';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;
