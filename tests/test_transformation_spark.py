"""Tests for PySpark ETL transformations, derived metrics, and partitioned Parquet writes."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager
from src.transformation.etl_job import SparkRetailETL
from src.validation.validator import BatchValidator


def test_spark_etl_transforms_and_partitions(
    test_config, sample_batch_csv: Path, spark_session
) -> None:
    lake = LakeManager(
        test_config.raw_zone,
        test_config.processed_zone,
        test_config.rejected_zone,
        test_config.curated_zone,
    )
    cp = CheckpointStore(test_config.checkpoint_db_path)
    cp.register_batch_start("batch_spark_01", str(sample_batch_csv), "sha_spark_01")

    validator = BatchValidator(lake, cp, test_config.reports_dir)
    _, valid_pq, _ = validator.validate_batch(sample_batch_csv, "batch_spark_01")

    etl = SparkRetailETL(
        spark=spark_session,
        lake_manager=lake,
        checkpoint_store=cp,
        special_non_product_codes=["D", "POST"],
    )
    res = etl.run_batch(valid_pq, batch_id="batch_spark_01", build_aggregates=True)

    assert res.curated_row_count == 3
    partition_dir = res.curated_transactions_path / "invoice_year_month=2011-11" / "batch_id=batch_spark_01"
    assert partition_dir.exists()

    curated_pdf = pd.read_parquet(partition_dir)
    assert len(curated_pdf) == 3
    assert curated_pdf["transaction_line_id"].nunique() == 3

    cancel_row = curated_pdf.loc[curated_pdf["invoice_no"] == "C536379"].iloc[0]
    assert bool(cancel_row["is_cancelled"]) is True
    assert bool(cancel_row["is_non_product_fee"]) is True
    assert float(cancel_row["cancelled_amount"]) == 27.5
    assert float(cancel_row["gross_amount"]) == 0.0
    assert float(cancel_row["line_total"]) == -27.5

    guest_row = curated_pdf.loc[curated_pdf["invoice_no"] == "536544"].iloc[0]
    assert bool(guest_row["is_guest_customer"]) is True
    assert guest_row["customer_id"] == "GUEST"
    assert guest_row["description"] == "UNKNOWN PRODUCT"

    assert res.monthly_summary_rows >= 1
    assert res.product_summary_rows >= 1
    assert res.customer_summary_rows >= 1
