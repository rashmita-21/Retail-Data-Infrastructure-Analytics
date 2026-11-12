-- Amazon Redshift Dimensional Star Schema DDL
-- Mirrors the local DuckDB warehouse with Redshift DISTKEY / SORTKEY optimizations

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(16) NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(16) NOT NULL,
    is_weekend BOOLEAN NOT NULL
)
DISTSTYLE ALL
SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS analytics.dim_country (
    country_key INTEGER IDENTITY(1,1) PRIMARY KEY,
    country_name VARCHAR(128) NOT NULL UNIQUE,
    is_unspecified BOOLEAN NOT NULL,
    region_group VARCHAR(64) NOT NULL
)
DISTSTYLE ALL
SORTKEY (country_name);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key INTEGER IDENTITY(1,1) PRIMARY KEY,
    stock_code VARCHAR(64) NOT NULL UNIQUE,
    product_description VARCHAR(512) NOT NULL,
    is_non_product_fee BOOLEAN NOT NULL,
    first_seen_date DATE,
    last_seen_date DATE,
    updated_at TIMESTAMP NOT NULL
)
DISTSTYLE ALL
SORTKEY (stock_code);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key INTEGER IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    is_guest BOOLEAN NOT NULL,
    primary_country VARCHAR(128),
    first_purchase_ts TIMESTAMP,
    last_purchase_ts TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (customer_id);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    transaction_line_id VARCHAR(64) PRIMARY KEY,
    invoice_no VARCHAR(32) NOT NULL,
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES analytics.dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES analytics.dim_product(product_key),
    country_key INTEGER NOT NULL REFERENCES analytics.dim_country(country_key),
    invoice_ts TIMESTAMP NOT NULL,
    invoice_hour SMALLINT NOT NULL,
    quantity INTEGER NOT NULL,
    abs_quantity INTEGER NOT NULL,
    unit_price DECIMAL(12, 4) NOT NULL,
    gross_amount DECIMAL(14, 4) NOT NULL,
    cancelled_amount DECIMAL(14, 4) NOT NULL,
    net_amount DECIMAL(14, 4) NOT NULL,
    is_cancelled BOOLEAN NOT NULL,
    batch_id VARCHAR(128) NOT NULL,
    is_late_arriving BOOLEAN NOT NULL,
    loaded_at TIMESTAMP NOT NULL
)
DISTSTYLE KEY
DISTKEY (customer_key)
COMPOUND SORTKEY (date_key, invoice_ts);

CREATE TABLE IF NOT EXISTS analytics.etl_batch_audit (
    batch_id VARCHAR(128) PRIMARY KEY,
    batch_type VARCHAR(32) NOT NULL,
    is_late_arriving BOOLEAN NOT NULL,
    rows_loaded BIGINT NOT NULL,
    min_invoice_ts TIMESTAMP,
    max_invoice_ts TIMESTAMP,
    loaded_at TIMESTAMP NOT NULL
);
