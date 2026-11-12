"""Tests for incremental batch loading, idempotency on reruns, and failure recovery."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.orchestration.pipeline import RetailPipelineOrchestrator


def _write_csv(path: Path, rows: list[dict]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_incremental_and_idempotent_reruns(test_config, tmp_path: Path, spark_session) -> None:
    batch1 = _write_csv(
        tmp_path / "b1.csv",
        [
            {
                "InvoiceNo": "500001",
                "StockCode": "SKU1",
                "Description": "Item One",
                "Quantity": 4,
                "InvoiceDate": "2011-10-01 10:00:00",
                "UnitPrice": 5.0,
                "CustomerID": "10001",
                "Country": "United Kingdom",
            }
        ],
    )
    batch2 = _write_csv(
        tmp_path / "b2.csv",
        [
            {
                "InvoiceNo": "500002",
                "StockCode": "SKU2",
                "Description": "Item Two",
                "Quantity": 2,
                "InvoiceDate": "2011-10-02 11:00:00",
                "UnitPrice": 15.0,
                "CustomerID": "10002",
                "Country": "Germany",
            }
        ],
    )

    orch = RetailPipelineOrchestrator(test_config, spark=spark_session)

    # First run of batch 1 and batch 2
    r1 = orch.process_batch(batch1, batch_id="b1", batch_type="backfill")
    r2 = orch.process_batch(batch2, batch_id="b2", batch_type="incremental")
    assert r1.status == "LOADED"
    assert r2.status == "LOADED"
    assert r2.warehouse_total_rows == 2

    # Second run of batch 2 without force -> skipped cleanly as duplicate
    r2_dup = orch.process_batch(batch2, batch_id="b2", batch_type="incremental", force=False)
    assert r2_dup.skipped_as_duplicate is True

    # Forced rerun of batch 2 -> re-executes all stages without creating duplicate rows
    r2_forced = orch.process_batch(batch2, batch_id="b2", batch_type="incremental", force=True)
    assert r2_forced.status == "LOADED"
    assert r2_forced.warehouse_total_rows == 2

    with duckdb.connect(str(test_config.warehouse_db_path), read_only=True) as conn:
        fact_count = conn.execute("SELECT COUNT(*) FROM analytics.fact_sales").fetchone()[0]
        net_sum = conn.execute("SELECT SUM(net_amount) FROM analytics.fact_sales").fetchone()[0]
        assert fact_count == 2
        assert float(net_sum) == 50.0
