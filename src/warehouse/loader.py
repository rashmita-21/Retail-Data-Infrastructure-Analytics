"""DuckDB dimensional warehouse loader with incremental and idempotent UPSERTs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from src.common.logging_utils import get_logger
from src.storage.checkpoint_store import CheckpointStore

logger = get_logger("warehouse.loader")

COUNTRY_REGION_CASE = """
CASE
    WHEN country_name = 'Unspecified' THEN 'Unspecified'
    WHEN country_name = 'United Kingdom' THEN 'United Kingdom'
    WHEN country_name IN ('USA', 'Canada', 'Brazil') THEN 'Americas'
    WHEN country_name IN ('Australia', 'Japan', 'Singapore', 'Hong Kong') THEN 'Asia-Pacific'
    WHEN country_name IN ('Israel', 'Lebanon', 'Saudi Arabia', 'United Arab Emirates', 'Bahrain', 'RSA') THEN 'Middle East & Africa'
    ELSE 'Europe'
END
"""


@dataclass
class WarehouseLoadResult:
    batch_id: str
    rows_upserted: int
    fact_total_rows: int
    dim_customer_count: int
    dim_product_count: int
    dim_date_count: int
    dim_country_count: int


class DuckDBWarehouseLoader:
    """Loads curated Parquet partitions into the DuckDB Star Schema idempotently."""

    def __init__(
        self,
        db_path: Path,
        ddl_path: Path,
        checkpoint_store: CheckpointStore,
    ) -> None:
        self.db_path = db_path
        self.ddl_path = ddl_path
        self.checkpoint_store = checkpoint_store
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def _ensure_schema(self) -> None:
        ddl_sql = self.ddl_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.execute(ddl_sql)
            # Ensure GUEST customer surrogate key = -1 exists
            conn.execute(
                """
                INSERT INTO analytics.dim_customer (
                    customer_key, customer_id, is_guest, primary_country,
                    first_purchase_ts, last_purchase_ts, updated_at
                )
                VALUES (-1, 'GUEST', TRUE, 'Unspecified', NULL, NULL, CURRENT_TIMESTAMP)
                ON CONFLICT (customer_id) DO NOTHING
                """
            )

    def load_batch(
        self,
        curated_transactions_dir: Path,
        batch_id: str,
        batch_type: str = "incremental",
        is_late_arriving: bool = False,
    ) -> WarehouseLoadResult:
        """
        Idempotently upsert dimensions and fact_sales for a single batch from curated Parquet.
        """
        glob_pattern = str(
            (curated_transactions_dir / "invoice_year_month=*" / f"batch_id={batch_id}" / "*.parquet").as_posix()
        )
        with self._connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    f"""
                    CREATE OR REPLACE TEMP TABLE stg_batch AS
                    SELECT *
                    FROM read_parquet('{glob_pattern}', hive_partitioning = true)
                    """
                )

                stg_count_row = conn.execute("SELECT COUNT(*) FROM stg_batch").fetchone()
                stg_count = int(stg_count_row[0]) if stg_count_row else 0
                if stg_count == 0:
                    raise ValueError(f"No curated Parquet records found for batch_id={batch_id}")

                # 1. Upsert dim_date
                conn.execute(
                    """
                    INSERT INTO analytics.dim_date (
                        date_key, full_date, year, quarter, month, month_name,
                        year_month, week_of_year, day_of_month, day_of_week, day_name, is_weekend
                    )
                    SELECT DISTINCT
                        CAST(STRFTIME(invoice_date, '%Y%m%d') AS INTEGER) AS date_key,
                        invoice_date AS full_date,
                        EXTRACT(YEAR FROM invoice_date)::INTEGER AS year,
                        EXTRACT(QUARTER FROM invoice_date)::INTEGER AS quarter,
                        EXTRACT(MONTH FROM invoice_date)::INTEGER AS month,
                        STRFTIME(invoice_date, '%B') AS month_name,
                        STRFTIME(invoice_date, '%Y-%m') AS year_month,
                        EXTRACT(WEEK FROM invoice_date)::INTEGER AS week_of_year,
                        EXTRACT(DAY FROM invoice_date)::INTEGER AS day_of_month,
                        EXTRACT(ISODOW FROM invoice_date)::INTEGER AS day_of_week,
                        STRFTIME(invoice_date, '%A') AS day_name,
                        EXTRACT(ISODOW FROM invoice_date) IN (6, 7) AS is_weekend
                    FROM stg_batch
                    ON CONFLICT (date_key) DO NOTHING
                    """
                )

                # 2. Upsert dim_country
                conn.execute(
                    f"""
                    INSERT INTO analytics.dim_country (
                        country_key, country_name, is_unspecified, region_group
                    )
                    SELECT
                        NEXTVAL('analytics.seq_country_key') AS country_key,
                        c.country_name,
                        (c.country_name = 'Unspecified') AS is_unspecified,
                        {COUNTRY_REGION_CASE.replace("country_name", "c.country_name")} AS region_group
                    FROM (
                        SELECT DISTINCT country AS country_name
                        FROM stg_batch
                    ) c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM analytics.dim_country d WHERE d.country_name = c.country_name
                    )
                    """
                )

                # 3. Upsert dim_product (insert new + update existing metadata)
                conn.execute(
                    """
                    CREATE OR REPLACE TEMP TABLE stg_products AS
                    SELECT
                        stock_code,
                        COALESCE(
                            MAX(CASE WHEN NOT is_missing_description THEN description END),
                            'UNKNOWN PRODUCT'
                        ) AS product_description,
                        BOOL_OR(is_non_product_fee) AS is_non_product_fee,
                        MIN(invoice_date) AS first_seen_date,
                        MAX(invoice_date) AS last_seen_date
                    FROM stg_batch
                    GROUP BY stock_code
                    """
                )
                conn.execute(
                    """
                    UPDATE analytics.dim_product AS d
                    SET
                        product_description = CASE
                            WHEN d.product_description = 'UNKNOWN PRODUCT'
                                 AND s.product_description <> 'UNKNOWN PRODUCT'
                            THEN s.product_description
                            ELSE d.product_description
                        END,
                        is_non_product_fee = (d.is_non_product_fee OR s.is_non_product_fee),
                        first_seen_date = LEAST(d.first_seen_date, s.first_seen_date),
                        last_seen_date = GREATEST(d.last_seen_date, s.last_seen_date),
                        updated_at = CURRENT_TIMESTAMP
                    FROM stg_products s
                    WHERE d.stock_code = s.stock_code
                    """
                )
                conn.execute(
                    """
                    INSERT INTO analytics.dim_product (
                        product_key, stock_code, product_description,
                        is_non_product_fee, first_seen_date, last_seen_date, updated_at
                    )
                    SELECT
                        NEXTVAL('analytics.seq_product_key'),
                        s.stock_code,
                        s.product_description,
                        s.is_non_product_fee,
                        s.first_seen_date,
                        s.last_seen_date,
                        CURRENT_TIMESTAMP
                    FROM stg_products s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM analytics.dim_product d WHERE d.stock_code = s.stock_code
                    )
                    """
                )

                # 4. Upsert dim_customer (excluding GUEST which stays -1, though timestamps update)
                conn.execute(
                    """
                    CREATE OR REPLACE TEMP TABLE stg_customers AS
                    SELECT
                        customer_id,
                        BOOL_OR(is_guest_customer) AS is_guest,
                        MAX(country) AS primary_country,
                        MIN(invoice_ts) AS first_purchase_ts,
                        MAX(invoice_ts) AS last_purchase_ts
                    FROM stg_batch
                    GROUP BY customer_id
                    """
                )
                conn.execute(
                    """
                    UPDATE analytics.dim_customer AS d
                    SET
                        primary_country = COALESCE(d.primary_country, s.primary_country),
                        first_purchase_ts = CASE
                            WHEN d.first_purchase_ts IS NULL THEN s.first_purchase_ts
                            ELSE LEAST(d.first_purchase_ts, s.first_purchase_ts)
                        END,
                        last_purchase_ts = CASE
                            WHEN d.last_purchase_ts IS NULL THEN s.last_purchase_ts
                            ELSE GREATEST(d.last_purchase_ts, s.last_purchase_ts)
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    FROM stg_customers s
                    WHERE d.customer_id = s.customer_id
                    """
                )
                conn.execute(
                    """
                    INSERT INTO analytics.dim_customer (
                        customer_key, customer_id, is_guest, primary_country,
                        first_purchase_ts, last_purchase_ts, updated_at
                    )
                    SELECT
                        NEXTVAL('analytics.seq_customer_key'),
                        s.customer_id,
                        s.is_guest,
                        s.primary_country,
                        s.first_purchase_ts,
                        s.last_purchase_ts,
                        CURRENT_TIMESTAMP
                    FROM stg_customers s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM analytics.dim_customer d WHERE d.customer_id = s.customer_id
                    )
                    """
                )

                # 5. Idempotent UPSERT into fact_sales
                conn.execute(
                    """
                    INSERT INTO analytics.fact_sales (
                        transaction_line_id, invoice_no, date_key, customer_key,
                        product_key, country_key, invoice_ts, invoice_hour,
                        quantity, abs_quantity, unit_price, gross_amount,
                        cancelled_amount, net_amount, is_cancelled,
                        batch_id, is_late_arriving, loaded_at
                    )
                    SELECT
                        s.transaction_line_id,
                        s.invoice_no,
                        CAST(STRFTIME(s.invoice_date, '%Y%m%d') AS INTEGER) AS date_key,
                        c.customer_key,
                        p.product_key,
                        co.country_key,
                        s.invoice_ts,
                        s.invoice_hour,
                        s.quantity,
                        s.abs_quantity,
                        CAST(s.unit_price AS DECIMAL(12, 4)),
                        CAST(s.gross_amount AS DECIMAL(14, 4)),
                        CAST(s.cancelled_amount AS DECIMAL(14, 4)),
                        CAST(s.line_total AS DECIMAL(14, 4)),
                        s.is_cancelled,
                        s.batch_id,
                        ? AS is_late_arriving,
                        CURRENT_TIMESTAMP AS loaded_at
                    FROM stg_batch s
                    JOIN analytics.dim_customer c ON s.customer_id = c.customer_id
                    JOIN analytics.dim_product p ON s.stock_code = p.stock_code
                    JOIN analytics.dim_country co ON s.country = co.country_name
                    ON CONFLICT (transaction_line_id) DO UPDATE SET
                        date_key = excluded.date_key,
                        customer_key = excluded.customer_key,
                        product_key = excluded.product_key,
                        country_key = excluded.country_key,
                        invoice_ts = excluded.invoice_ts,
                        invoice_hour = excluded.invoice_hour,
                        quantity = excluded.quantity,
                        abs_quantity = excluded.abs_quantity,
                        unit_price = excluded.unit_price,
                        gross_amount = excluded.gross_amount,
                        cancelled_amount = excluded.cancelled_amount,
                        net_amount = excluded.net_amount,
                        is_cancelled = excluded.is_cancelled,
                        batch_id = excluded.batch_id,
                        is_late_arriving = excluded.is_late_arriving,
                        loaded_at = excluded.loaded_at
                    """,
                    [is_late_arriving],
                )

                # 6. Record warehouse batch audit
                conn.execute(
                    """
                    INSERT INTO analytics.etl_batch_audit (
                        batch_id, batch_type, is_late_arriving, rows_loaded,
                        min_invoice_ts, max_invoice_ts, loaded_at
                    )
                    SELECT
                        ?,
                        ?,
                        ?,
                        COUNT(*),
                        MIN(invoice_ts),
                        MAX(invoice_ts),
                        CURRENT_TIMESTAMP
                    FROM stg_batch
                    ON CONFLICT (batch_id) DO UPDATE SET
                        batch_type = excluded.batch_type,
                        is_late_arriving = excluded.is_late_arriving,
                        rows_loaded = excluded.rows_loaded,
                        min_invoice_ts = excluded.min_invoice_ts,
                        max_invoice_ts = excluded.max_invoice_ts,
                        loaded_at = excluded.loaded_at
                    """,
                    [batch_id, batch_type, is_late_arriving],
                )

                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            counts = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM analytics.fact_sales),
                    (SELECT COUNT(*) FROM analytics.dim_customer),
                    (SELECT COUNT(*) FROM analytics.dim_product),
                    (SELECT COUNT(*) FROM analytics.dim_date),
                    (SELECT COUNT(*) FROM analytics.dim_country)
                """
            ).fetchone()
            max_ts_row = conn.execute("SELECT MAX(invoice_ts)::VARCHAR FROM stg_batch").fetchone()

        assert counts is not None
        if max_ts_row and max_ts_row[0]:
            self.checkpoint_store.advance_high_watermark(max_ts_row[0], batch_id=batch_id)

        self.checkpoint_store.update_batch_stage(
            batch_id=batch_id,
            status="LOADED",
            record_count_warehouse=stg_count,
        )
        self.checkpoint_store.record_step_checkpoint(batch_id, "warehouse_load", "COMPLETED")

        result = WarehouseLoadResult(
            batch_id=batch_id,
            rows_upserted=stg_count,
            fact_total_rows=int(counts[0]),
            dim_customer_count=int(counts[1]),
            dim_product_count=int(counts[2]),
            dim_date_count=int(counts[3]),
            dim_country_count=int(counts[4]),
        )
        logger.info(
            "Warehouse batch load completed",
            extra={
                "batch_id": batch_id,
                "rows_upserted": result.rows_upserted,
                "fact_total_rows": result.fact_total_rows,
                "is_late_arriving": is_late_arriving,
            },
        )
        return result
