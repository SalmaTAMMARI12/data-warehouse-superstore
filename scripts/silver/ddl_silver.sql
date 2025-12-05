CREATE TABLE silver.cleaned_sales (
    row_id INT PRIMARY KEY,

    -- Identifiants
    order_id VARCHAR(50) NOT NULL,

    -- Dates
    order_date DATE NOT NULL,
    order_date_key INT GENERATED ALWAYS AS (
        EXTRACT(YEAR FROM order_date)::INT * 10000 +
        EXTRACT(MONTH FROM order_date)::INT * 100 +
        EXTRACT(DAY FROM order_date)::INT
    ) STORED,

    ship_date DATE NOT NULL,
    ship_date_key INT GENERATED ALWAYS AS (
        EXTRACT(YEAR FROM ship_date)::INT * 10000 +
        EXTRACT(MONTH FROM ship_date)::INT * 100 +
        EXTRACT(DAY FROM ship_date)::INT
    ) STORED,

    ship_mode VARCHAR(50) NOT NULL,

    -- Customer
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    segment VARCHAR(50) NOT NULL,

    -- Location
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    postal_code VARCHAR(50),
    region VARCHAR(50),

    -- Product
    product_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    sub_category VARCHAR(100) NOT NULL,
    product_name VARCHAR(500) NOT NULL,

    -- Measures
    sales DECIMAL(12,2) NOT NULL,
    quantity INT NOT NULL,
    discount DECIMAL(5,2) NOT NULL,
    profit DECIMAL(12,2) NOT NULL,

    -- Validations
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Quality checks
    CONSTRAINT chk_dates CHECK (ship_date >= order_date),
    CONSTRAINT chk_sales CHECK (sales >= 0),
    CONSTRAINT chk_quantity CHECK (quantity > 0),
    CONSTRAINT chk_discount CHECK (discount >= 0 AND discount <= 1)
);
