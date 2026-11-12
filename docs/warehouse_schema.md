# Data Warehouse Dimensional Schema

## Overview

The local analytical warehouse runs on **DuckDB** (`data/warehouse/retail_warehouse.duckdb`) inside schema `analytics`, with a direct cloud counterpart in **Amazon Redshift** (`infrastructure/aws/redshift/01_create_schemas_and_tables.sql`).

## Star Schema Entity-Relationship Diagram

```mermaid
erDiagram
    fact_sales {
        varchar transaction_line_id PK
        varchar invoice_no
        integer date_key FK
        integer customer_key FK
        integer product_key FK
        integer country_key FK
        timestamp invoice_ts
        integer invoice_hour
        integer quantity
        integer abs_quantity
        decimal unit_price
        decimal gross_amount
        decimal cancelled_amount
        decimal net_amount
        boolean is_cancelled
        varchar batch_id
        boolean is_late_arriving
        timestamp loaded_at
    }
    dim_date {
        integer date_key PK
        date full_date
        integer year
        integer quarter
        integer month
        varchar month_name
        varchar year_month
        integer week_of_year
        integer day_of_month
        integer day_of_week
        varchar day_name
        boolean is_weekend
    }
    dim_customer {
        integer customer_key PK
        varchar customer_id
        boolean is_guest
        varchar primary_country
        timestamp first_purchase_ts
        timestamp last_purchase_ts
        timestamp updated_at
    }
    dim_product {
        integer product_key PK
        varchar stock_code
        varchar product_description
        boolean is_non_product_fee
        date first_seen_date
        date last_seen_date
        timestamp updated_at
    }
    dim_country {
        integer country_key PK
        varchar country_name
        boolean is_unspecified
        varchar region_group
    }

    dim_date ||--o{ fact_sales : "date_key"
    dim_customer ||--o{ fact_sales : "customer_key"
    dim_product ||--o{ fact_sales : "product_key"
    dim_country ||--o{ fact_sales : "country_key"
```

## Table Grain & Design Decisions

| Table | Grain | Primary Key | Design Notes |
| --- | --- | --- | --- |
| `analytics.fact_sales` | One validated invoice line item | `transaction_line_id` (SHA-256) | Retains both positive sales (`is_cancelled = false`) and credit cancellations (`is_cancelled = true`) so net, gross, and cancellation metrics reconcile cleanly. |
| `analytics.dim_customer` | One distinct `customer_id` (+ `-1` for `GUEST`) | `customer_key` (INTEGER) | ~24.9% of raw UCI transactions lack `CustomerID`. Mapping them to `customer_key = -1` preserves total store revenue while letting customer RFM/CLV queries filter `NOT is_guest`. |
| `analytics.dim_product` | One distinct `stock_code` | `product_key` (INTEGER) | Flags non-merchandise service codes (`POST`, `D`, `M`, `BANK CHARGES`, `DOT`, `AMAZONFEE`, `CRUK`, `S`, `PADS`) via `is_non_product_fee`. |
| `analytics.dim_date` | One calendar day | `date_key` (`YYYYMMDD`) | Supports calendar, weekday/weekend, quarter, and cohort aggregations. |
| `analytics.dim_country` | One distinct country string | `country_key` (INTEGER) | Includes `region_group` classification and preserves `Unspecified` for audit completeness. |
| `analytics.etl_batch_audit` | One loaded batch | `batch_id` (VARCHAR) | Records batch lineage, row counts, `is_late_arriving` flag, and min/max timestamps. |
