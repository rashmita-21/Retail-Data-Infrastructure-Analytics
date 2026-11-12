"""Tests ensuring every analytical SQL query executes cleanly against the warehouse."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.run_analytics import run_all_queries
from src.orchestration.pipeline import RetailPipelineOrchestrator


def test_all_analytical_sql_queries_execute(test_config, tmp_path: Path, spark_session) -> None:
    multi_csv = tmp_path / "multi_month.csv"
    pd.DataFrame(
        [
            {
                "InvoiceNo": "520001",
                "StockCode": "P001",
                "Description": "LAMP",
                "Quantity": 5,
                "InvoiceDate": "2011-09-05 10:15:00",
                "UnitPrice": 20.0,
                "CustomerID": "30001",
                "Country": "United Kingdom",
            },
            {
                "InvoiceNo": "520002",
                "StockCode": "P002",
                "Description": "MUG",
                "Quantity": 10,
                "InvoiceDate": "2011-10-10 14:30:00",
                "UnitPrice": 5.0,
                "CustomerID": "30001",
                "Country": "United Kingdom",
            },
            {
                "InvoiceNo": "C520003",
                "StockCode": "P002",
                "Description": "MUG",
                "Quantity": -2,
                "InvoiceDate": "2011-10-12 16:00:00",
                "UnitPrice": 5.0,
                "CustomerID": "30001",
                "Country": "United Kingdom",
            },
            {
                "InvoiceNo": "520004",
                "StockCode": "P001",
                "Description": "LAMP",
                "Quantity": 2,
                "InvoiceDate": "2011-10-15 11:20:00",
                "UnitPrice": 20.0,
                "CustomerID": "30002",
                "Country": "France",
            },
            {
                "InvoiceNo": "520005",
                "StockCode": "P003",
                "Description": "CLOCK",
                "Quantity": 1,
                "InvoiceDate": "2011-10-18 15:00:00",
                "UnitPrice": 40.0,
                "CustomerID": None,
                "Country": "Germany",
            },
        ]
    ).to_csv(multi_csv, index=False)

    orch = RetailPipelineOrchestrator(test_config, spark=spark_session)
    orch.process_batch(multi_csv, batch_id="batch_sql_01", batch_type="backfill")

    analytics_dir = test_config.sql_dir / "analytics"
    sql_files = list(analytics_dir.glob("*.sql"))
    assert len(sql_files) >= 15

    summary = run_all_queries(
        db_path=test_config.warehouse_db_path,
        analytics_dir=analytics_dir,
        reports_dir=test_config.reports_dir,
    )
    assert summary["queries_executed"] == len(sql_files)
    assert (test_config.reports_dir / "analytics_execution_summary.json").exists()
