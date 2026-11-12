-- Idempotent Redshift Staging COPY + MERGE from Curated S3 Parquet
-- Replace placeholders (${S3_CURATED_BUCKET}, ${REDSHIFT_IAM_ROLE_ARN}) via deployment script

CREATE TEMP TABLE stg_curated_sales (
    transaction_line_id VARCHAR(64),
    invoice_no VARCHAR(32),
    stock_code VARCHAR(64),
    description VARCHAR(512),
    quantity INTEGER,
    abs_quantity INTEGER,
    unit_price DOUBLE PRECISION,
    line_total DOUBLE PRECISION,
    gross_amount DOUBLE PRECISION,
    cancelled_amount DOUBLE PRECISION,
    invoice_ts TIMESTAMP,
    invoice_date DATE,
    invoice_year INTEGER,
    invoice_month INTEGER,
    invoice_day_of_week INTEGER,
    invoice_hour INTEGER,
    customer_id VARCHAR(64),
    is_guest_customer BOOLEAN,
    is_missing_description BOOLEAN,
    is_cancelled BOOLEAN,
    is_non_product_fee BOOLEAN,
    country VARCHAR(128),
    processed_at_utc TIMESTAMP
);

COPY stg_curated_sales
FROM 's3://${S3_CURATED_BUCKET}/transactions_partitioned/'
IAM_ROLE '${REDSHIFT_IAM_ROLE_ARN}'
FORMAT AS PARQUET;

BEGIN TRANSACTION;

INSERT INTO analytics.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    year_month, week_of_year, day_of_month, day_of_week, day_name, is_weekend
)
SELECT DISTINCT
    CAST(TO_CHAR(invoice_date, 'YYYYMMDD') AS INTEGER),
    invoice_date,
    EXTRACT(YEAR FROM invoice_date)::INTEGER,
    EXTRACT(QUARTER FROM invoice_date)::INTEGER,
    EXTRACT(MONTH FROM invoice_date)::INTEGER,
    TRIM(TO_CHAR(invoice_date, 'Month')),
    TO_CHAR(invoice_date, 'YYYY-MM'),
    EXTRACT(WEEK FROM invoice_date)::INTEGER,
    EXTRACT(DAY FROM invoice_date)::INTEGER,
    EXTRACT(DOW FROM invoice_date)::INTEGER,
    TRIM(TO_CHAR(invoice_date, 'Day')),
    EXTRACT(DOW FROM invoice_date) IN (0, 6)
FROM stg_curated_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.dim_date d WHERE d.full_date = s.invoice_date
);

INSERT INTO analytics.dim_country (country_name, is_unspecified, region_group)
SELECT DISTINCT
    s.country,
    (s.country = 'Unspecified'),
    CASE
        WHEN s.country = 'Unspecified' THEN 'Unspecified'
        WHEN s.country = 'United Kingdom' THEN 'United Kingdom'
        WHEN s.country IN ('USA', 'Canada', 'Brazil') THEN 'Americas'
        WHEN s.country IN ('Australia', 'Japan', 'Singapore', 'Hong Kong') THEN 'Asia-Pacific'
        WHEN s.country IN ('Israel', 'Lebanon', 'Saudi Arabia', 'United Arab Emirates', 'Bahrain', 'RSA') THEN 'Middle East & Africa'
        ELSE 'Europe'
    END
FROM stg_curated_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.dim_country c WHERE c.country_name = s.country
);

INSERT INTO analytics.dim_product (
    stock_code, product_description, is_non_product_fee, first_seen_date, last_seen_date, updated_at
)
SELECT
    s.stock_code,
    MAX(s.description),
    BOOL_OR(s.is_non_product_fee),
    MIN(s.invoice_date),
    MAX(s.invoice_date),
    GETDATE()
FROM stg_curated_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.dim_product p WHERE p.stock_code = s.stock_code
)
GROUP BY s.stock_code;

INSERT INTO analytics.dim_customer (
    customer_id, is_guest, primary_country, first_purchase_ts, last_purchase_ts, updated_at
)
SELECT
    s.customer_id,
    BOOL_OR(s.is_guest_customer),
    MAX(s.country),
    MIN(s.invoice_ts),
    MAX(s.invoice_ts),
    GETDATE()
FROM stg_curated_sales s
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.dim_customer c WHERE c.customer_id = s.customer_id
)
GROUP BY s.customer_id;

DELETE FROM analytics.fact_sales
USING stg_curated_sales s
WHERE analytics.fact_sales.transaction_line_id = s.transaction_line_id;

INSERT INTO analytics.fact_sales (
    transaction_line_id, invoice_no, date_key, customer_key, product_key, country_key,
    invoice_ts, invoice_hour, quantity, abs_quantity, unit_price,
    gross_amount, cancelled_amount, net_amount, is_cancelled,
    batch_id, is_late_arriving, loaded_at
)
SELECT
    s.transaction_line_id,
    s.invoice_no,
    CAST(TO_CHAR(s.invoice_date, 'YYYYMMDD') AS INTEGER),
    c.customer_key,
    p.product_key,
    co.country_key,
    s.invoice_ts,
    s.invoice_hour::SMALLINT,
    s.quantity,
    s.abs_quantity,
    CAST(s.unit_price AS DECIMAL(12, 4)),
    CAST(s.gross_amount AS DECIMAL(14, 4)),
    CAST(s.cancelled_amount AS DECIMAL(14, 4)),
    CAST(s.line_total AS DECIMAL(14, 4)),
    s.is_cancelled,
    'redshift_s3_copy',
    FALSE,
    GETDATE()
FROM stg_curated_sales s
JOIN analytics.dim_customer c ON s.customer_id = c.customer_id
JOIN analytics.dim_product p ON s.stock_code = p.stock_code
JOIN analytics.dim_country co ON s.country = co.country_name;

END TRANSACTION;
