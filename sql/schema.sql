-- sql/schema.sql
-- Retail Analytics Schema for PostgreSQL
-- Star Schema Design: Fact + Dimension tables


-- =========================
-- 0. Drop Existing Tables
-- =========================
DROP TABLE IF EXISTS sales_fact CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE; -- Optional for advanced time analysis

-- =========================
-- 1. Dimension Table: dim_customer
-- =========================
CREATE TABLE dim_customer (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    city VARCHAR(100),
    signup_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_customer IS 'Dimension table containing customer information';

-- =========================
-- 2. Dimension Table: dim_store
-- =========================
CREATE TABLE dim_store (
    store_id BIGSERIAL PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    region VARCHAR(50),
    manager_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_store IS 'Dimension table containing store information';

-- =========================
-- 3. Dimension Table: dim_product
-- =========================
CREATE TABLE dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_cost DECIMAL(10,2) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_product IS 'Dimension table containing product information';

-- =========================
-- 4. Dimension Table: dim_date (Optional)
-- =========================
-- Useful for advanced time-series queries
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    is_weekend BOOLEAN
);

COMMENT ON TABLE dim_date IS 'Optional date dimension table for advanced time analysis';

-- =========================
-- 5. Fact Table: sales_fact
-- =========================
CREATE TABLE sales_fact (
    transaction_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    store_id BIGINT NOT NULL,
    sale_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    profit DECIMAL(12,2),
    discount_amount DECIMAL(10,2) DEFAULT 0,
    promotion_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Foreign Keys
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_product  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    CONSTRAINT fk_store    FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
    -- Optional: CONSTRAINT fk_date   FOREIGN KEY (sale_date) REFERENCES dim_date(date_id)
);

COMMENT ON TABLE sales_fact IS 'Fact table containing sales transactions';
COMMENT ON COLUMN sales_fact.profit IS 'Calculated as total_amount - (quantity * unit_cost from dim_product). Can be computed during ETL';

-- =========================
-- 6. Indexes for Performance
-- =========================
CREATE INDEX idx_sales_customer ON sales_fact(customer_id);
CREATE INDEX idx_sales_product  ON sales_fact(product_id);
CREATE INDEX idx_sales_store    ON sales_fact(store_id);
CREATE INDEX idx_sales_date     ON sales_fact(sale_date);

-- =========================
-- Notes
-- =========================
-- 1. Surrogate keys (BIGSERIAL) used for dimensions for realistic warehouse modeling.
-- 2. NOT NULL constraints added to critical columns for data integrity.
-- 3. Timestamps (created_at) added to track ETL/insert times.
-- 4. Optional dim_date included for advanced analytics, but queries can use sale_date directly.
-- 5. Discount and promotion columns added to simulate real retail datasets.
-- 6. Profit can be precomputed during ETL or calculated on-demand in queries.
-- 7. Indexes improve query performance on joins and aggregations.





















-- sql/schema.sql
-- Schema Definition for Retail Analytics Dashboard
-- This script creates the necessary tables in a PostgreSQL database.

-- Drop tables if they exist (for a clean start during development)
-- Be careful with this in a production environment!
DROP TABLE IF EXISTS sales_fact CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE; -- Optional but often useful for time-based analysis


-- 1. Dimension Table: dim_customer
-- Contains customer attributes
CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    signup_date DATE
);

-- 2. Dimension Table: dim_store
-- Contains store attributes
CREATE TABLE dim_store (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(50),
    manager_name VARCHAR(255)
);

-- 3. Dimension Table: dim_product
-- Contains product attributes
CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_cost DECIMAL(10, 2), -- Cost price
    unit_price DECIMAL(10, 2)  -- Selling price
);

-- 4. Dimension Table: dim_date (Optional for advanced date analysis)
-- This table can be pre-populated with dates and their attributes (year, quarter, month, day of week, etc.)
-- For simplicity in this project, we might just use the date from the fact table directly in queries.
-- CREATE TABLE dim_date (
--     date_id DATE PRIMARY KEY,
--     full_date DATE,
--     year INTEGER,
--     quarter INTEGER,
--     month INTEGER,
--     month_name VARCHAR(10),
--     day_of_month INTEGER,
--     day_of_week INTEGER,
--     day_name VARCHAR(10),
--     is_weekend BOOLEAN
-- );

-- 5. Fact Table: sales_fact
-- Contains the core transactional data
CREATE TABLE sales_fact (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    -- date_id DATE, -- If using dim_date
    sale_date DATE, -- Using date directly
    quantity INTEGER,
    unit_price DECIMAL(10, 2), -- Price at time of sale
    total_amount DECIMAL(12, 2), -- Quantity * Unit Price
    -- Calculated field
    profit DECIMAL(12, 2) -- Total Amount - (Quantity * Unit Cost)
    -- Foreign Key Constraints (optional in star schemas for performance, but good for integrity)
    -- CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    -- CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    -- CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
    -- CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id) -- If using dim_date
);

-- Optional: Add indexes on foreign key columns for faster joins if constraints are not used
-- CREATE INDEX idx_sales_customer ON sales_fact(customer_id);
-- CREATE INDEX idx_sales_store ON sales_fact(store_id);
-- CREATE INDEX idx_sales_product ON sales_fact(product_id);
-- CREATE INDEX idx_sales_date ON sales_fact(sale_date);

COMMENT ON TABLE dim_customer IS 'Dimension table containing customer information.';
COMMENT ON TABLE dim_store IS 'Dimension table containing store information.';
COMMENT ON TABLE dim_product IS 'Dimension table containing product information.';
COMMENT ON TABLE sales_fact IS 'Fact table containing sales transaction data.';

COMMENT ON COLUMN sales_fact.profit IS 'Calculated as total_amount - (quantity * unit_cost from dim_product). Requires join during ETL or calculation.';

-- Note: The foreign key constraints are commented out for performance in analytical queries,
-- but they ensure data integrity. You can add them back if strict integrity is needed.
-- The indexes are also commented out for the initial load; add them after data is loaded for querying performance.
