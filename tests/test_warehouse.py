"""Tests for DuckDB Star Schema integrity, keys, dimensions, and foreign-key joins."""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.orchestration.pipeline import RetailPipelineOrchestrator


def test_warehouse_star_schema_integrity(
    test_config, sample_batch_csv: Path, spark_session
) -> None:
    orch = RetailPipelineOrchestrator(test_config, spark=spark_session)
    res = orch.process_batch(sample_batch_csv, batch_id="batch_wh_01", batch_type="backfill")
    assert res.warehouse_total_rows == 3

    with duckdb.connect(str(test_config.warehouse_db_path), read_only=True) as conn:
        # Verify referential integrity across all 4 dimensions
        orphans = conn.execute(
            """
            SELECT COUNT(*)
            FROM analytics.fact_sales f
            LEFT JOIN analytics.dim_date d ON f.date_key = d.date_key
            LEFT JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
            LEFT JOIN analytics.dim_product p ON f.product_key = p.product_key
            LEFT JOIN analytics.dim_country co ON f.country_key = co.country_key
            WHERE d.date_key IS NULL
               OR c.customer_key IS NULL
               OR p.product_key IS NULL
               OR co.country_key IS NULL
            """
        ).fetchone()[0]
        assert orphans == 0

        # Verify GUEST surrogate key = -1 is linked properly
        guest_key = conn.execute(
            """
            SELECT c.customer_key
            FROM analytics.fact_sales f
            JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
            WHERE f.invoice_no = '536544'
            """
        ).fetchone()[0]
        assert guest_key == -1

        # Verify batch audit record exists
        audit = conn.execute(
            "SELECT rows_loaded FROM analytics.etl_batch_audit WHERE batch_id = 'batch_wh_01'"
        ).fetchone()
        assert audit is not None and audit[0] == 3
