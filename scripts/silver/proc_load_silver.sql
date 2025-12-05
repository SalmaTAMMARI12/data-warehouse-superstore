CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := NOW();
    batch_end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer: cleaned_sales';
    RAISE NOTICE '================================================';

    -- Truncate Silver table
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.cleaned_sales';
    TRUNCATE TABLE silver.cleaned_sales;
    end_time := NOW();
    RAISE NOTICE '>> Truncate Duration: % seconds', 
                 EXTRACT(SECOND FROM end_time - start_time);

    -- Insert transformed data from Bronze
    start_time := NOW();
    RAISE NOTICE '>> Inserting Data Into: silver.cleaned_sales';

    INSERT INTO silver.cleaned_sales (
        row_id,
        order_id,
        order_date,
        ship_date,
        ship_mode,
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region,
        product_id,
        category,
        sub_category,
        product_name,
        sales,
        quantity,
        discount,
        profit
    )
    SELECT
        row_id::INT,
        TRIM(order_id),

        /* Conversion sécurisée des dates */
        CASE WHEN order_date IS NULL OR order_date = '' 
             THEN NULL
             ELSE TO_DATE(order_date, 'MM/DD/YYYY')
        END,

        CASE WHEN ship_date IS NULL OR ship_date = '' 
             THEN NULL
             ELSE TO_DATE(ship_date, 'MM/DD/YYYY')
        END,

        UPPER(TRIM(ship_mode)),
        TRIM(customer_id),
        TRIM(customer_name),
        UPPER(TRIM(segment)),
        TRIM(country),
        TRIM(city),
        TRIM(state),
        TRIM(postal_code),
        TRIM(region),
        TRIM(product_id),
        UPPER(TRIM(category)),
        UPPER(TRIM(sub_category)),
        TRIM(product_name),

        /* Cast correct : les colonnes arrivent en TEXT dans bronze */
        COALESCE(sales::NUMERIC(12,2), 0),
        COALESCE(quantity::INT, 0),
        COALESCE(discount::NUMERIC(5,2), 0),
        COALESCE(profit::NUMERIC(12,2), 0)

    FROM bronze.raw_sales
    WHERE order_id IS NOT NULL;

    end_time := NOW();
    RAISE NOTICE '>> Insert Duration: % seconds', 
                 EXTRACT(SECOND FROM end_time - start_time);

    batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Silver Layer cleaned_sales Loaded Successfully';
    RAISE NOTICE '   - Total Duration: % seconds', 
                 EXTRACT(SECOND FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING SILVER LAYER LOAD';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;
