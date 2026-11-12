"""Tests for late-arriving historical data detection, partitioning, and watermark safety."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.orchestration.pipeline import RetailPipelineOrchestrator


def test_late_arriving_batch_preserves_watermark_and_backfills_history(
    test_config, tmp_path: Path, spark_session
) -> None:
    initial_csv = tmp_path / "initial.csv"
    pd.DataFrame(
        [
            {
                "InvoiceNo": "510001",
                "StockCode": "SKU_A",
                "Description": "Product A",
                "Quantity": 3,
                "InvoiceDate": "2011-11-15 12:00:00",
                "UnitPrice": 10.0,
                "CustomerID": "20001",
                "Country": "France",
            }
        ]
    ).to_csv(initial_csv, index=False)

    late_csv = tmp_path / "late.csv"
    pd.DataFrame(
        [
            {
                "InvoiceNo": "490001",
                "StockCode": "SKU_A",
                "Description": "Product A",
                "Quantity": 1,
                "InvoiceDate": "2011-05-10 09:30:00",
                "UnitPrice": 10.0,
                "CustomerID": "20001",
                "Country": "France",
            }
        ]
    ).to_csv(late_csv, index=False)

    orch = RetailPipelineOrchestrator(test_config, spark=spark_session)
    res_init = orch.process_batch(initial_csv, batch_id="b_init", batch_type="incremental")
    assert res_init.is_late_arriving is False
    hw_before = orch.checkpoint_store.get_high_watermark()
    assert hw_before is not None and hw_before.startswith("2011-11-15")

    res_late = orch.process_batch(late_csv, batch_id="b_late", batch_type="late_arriving")
    assert res_late.is_late_arriving is True
    assert res_late.warehouse_total_rows == 2

    # High watermark must NOT regress to 2011-05-10
    hw_after = orch.checkpoint_store.get_high_watermark()
    assert hw_after == hw_before

    # Historical Parquet partition exists separately from 2011-11
    assert (test_config.curated_zone / "transactions_partitioned" / "invoice_year_month=2011-05" / "batch_id=b_late").exists()

    # Customer first_purchase_ts updated to May 2011 while last_purchase_ts remains Nov 2011
    with duckdb.connect(str(test_config.warehouse_db_path), read_only=True) as conn:
        row = conn.execute(
            """
            SELECT STRFTIME(first_purchase_ts, '%Y-%m-%d'), STRFTIME(last_purchase_ts, '%Y-%m-%d')
            FROM analytics.dim_customer
            WHERE customer_id = '20001'
            """
        ).fetchone()
        assert row == ("2011-05-10", "2011-11-15")
        late_flag = conn.execute(
            "SELECT is_late_arriving FROM analytics.fact_sales WHERE batch_id = 'b_late'"
        ).fetchone()[0]
        assert late_flag is True
