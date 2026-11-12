# System Architecture

## Overview

The **Retail Data Infrastructure & Analytics Platform** implements a Lakehouse-to-Warehouse architecture modeled after modern AWS data platforms while remaining 100% executable locally without cloud billing or external infrastructure dependencies.

## End-to-End Pipeline Architecture

```mermaid
flowchart LR
    SRC["UCI Online Retail<br/>(Online Retail.xlsx)"] --> SIM["Historical Batch Simulator<br/>(Backfill + 15 Daily + Late Batch)"]
    SIM --> ING["Raw Ingester<br/>(Schema Check + SHA-256)"]
    ING --> BRONZE["Bronze / Raw Zone<br/>(data/lake/raw/)"]
    ING <--> CP["SQLite Checkpoint Store<br/>(Watermarks + Batch State)"]

    BRONZE --> DQ["Data Quality Engine<br/>(Rules + Deduplication)"]
    DQ --> SILVER["Silver / Processed Zone<br/>(Parquet)"]
    DQ --> REJ["Quarantine / Rejected Zone<br/>(Parquet + DQ JSON)"]

    SILVER --> SPARK["PySpark ETL Engine<br/>(Cleansing + SHA-256 PK + Metrics)"]
    SPARK --> GOLD["Gold / Curated Zone<br/>(Partitioned Parquet:<br/>invoice_year_month / batch_id)"]

    GOLD --> WH["DuckDB Star Schema<br/>(Idempotent MERGE/UPSERT)"]
    WH --> SQL["16 Analytical SQL Queries<br/>(RFM, Cohorts, Pareto, Trends)"]
    CP <--> WH
```

## AWS Cloud-Ready Target Architecture

```mermaid
flowchart LR
    S3RAW["Amazon S3<br/>Raw Bucket"] -->|"ObjectCreated"| LAMBDA["AWS Lambda<br/>Ingestion Trigger"]
    LAMBDA -->|"StartJobRun"| GLUE1["AWS Glue 4.0<br/>Bronze-to-Silver DQ"]
    GLUE1 --> S3PROC["Amazon S3<br/>Processed Bucket"]
    GLUE1 --> S3REJ["Amazon S3<br/>Rejected Bucket"]
    S3PROC --> GLUE2["AWS Glue 4.0<br/>Silver-to-Gold ETL"]
    GLUE2 --> S3CUR["Amazon S3<br/>Curated Partitioned Parquet"]
    S3CUR --> CRAWLER["AWS Glue Crawler<br/>& Data Catalog"]
    S3CUR -->|"COPY Parquet + MERGE"| RS["Amazon Redshift<br/>Star Schema"]
```

## Incremental Processing, Idempotency & Late-Arriving Data

1. **Batch & File Fingerprinting**: Every incoming CSV is fingerprinted with SHA-256. Re-ingesting an already-`LOADED` batch is skipped unless `--force` is passed.
2. **Partitioned Curated Lake**: PySpark writes curated transactions partitioned by `(invoice_year_month, batch_id)` with `spark.sql.sources.partitionOverwriteMode = dynamic`, ensuring forced reruns only replace that specific batch's partition slices.
3. **Monotonic Watermarking**: `CheckpointStore` tracks `high_watermark_ts` (`MAX(InvoiceDate)`). Late-arriving batches (`min_invoice_ts < high_watermark_ts`) are flagged `is_late_arriving = true`, backfilled into historical Parquet partitions and `analytics.fact_sales`, and update `dim_customer.first_purchase_ts = LEAST(...)` without regressing the high watermark.
4. **Warehouse Idempotency**: Every curated row receives a deterministic SHA-256 `transaction_line_id`. DuckDB loads use transactional `INSERT ... ON CONFLICT (transaction_line_id) DO UPDATE`, guaranteeing zero duplicate fact rows across retries or reruns.
