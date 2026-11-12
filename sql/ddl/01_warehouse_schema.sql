-- Retail Data Infrastructure & Analytics Platform
-- DuckDB Dimensional Star Schema DDL

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE SEQUENCE IF NOT EXISTS analytics.seq_customer_key START 1;
CREATE SEQUENCE IF NOT EXISTS analytics.seq_product_key START 1;
CREATE SEQUENCE IF NOT EXISTS analytics.seq_country_key START 1;

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR NOT NULL,
    year_month VARCHAR NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_country (
    country_key INTEGER PRIMARY KEY,
    country_name VARCHAR UNIQUE NOT NULL,
    is_unspecified BOOLEAN NOT NULL,
    region_group VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key INTEGER PRIMARY KEY,
    stock_code VARCHAR UNIQUE NOT NULL,
    product_description VARCHAR NOT NULL,
    is_non_product_fee BOOLEAN NOT NULL,
    first_seen_date DATE,
    last_seen_date DATE,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key INTEGER PRIMARY KEY,
    customer_id VARCHAR UNIQUE NOT NULL,
    is_guest BOOLEAN NOT NULL,
    primary_country VARCHAR,
    first_purchase_ts TIMESTAMP,
    last_purchase_ts TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    transaction_line_id VARCHAR PRIMARY KEY,
    invoice_no VARCHAR NOT NULL,
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES analytics.dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES analytics.dim_product(product_key),
    country_key INTEGER NOT NULL REFERENCES analytics.dim_country(country_key),
    invoice_ts TIMESTAMP NOT NULL,
    invoice_hour INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    abs_quantity INTEGER NOT NULL,
    unit_price DECIMAL(12, 4) NOT NULL,
    gross_amount DECIMAL(14, 4) NOT NULL,
    cancelled_amount DECIMAL(14, 4) NOT NULL,
    net_amount DECIMAL(14, 4) NOT NULL,
    is_cancelled BOOLEAN NOT NULL,
    batch_id VARCHAR NOT NULL,
    is_late_arriving BOOLEAN NOT NULL,
    loaded_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.etl_batch_audit (
    batch_id VARCHAR PRIMARY KEY,
    batch_type VARCHAR NOT NULL,
    is_late_arriving BOOLEAN NOT NULL,
    rows_loaded INTEGER NOT NULL,
    min_invoice_ts TIMESTAMP,
    max_invoice_ts TIMESTAMP,
    loaded_at TIMESTAMP NOT NULL
);
