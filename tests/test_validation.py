"""Tests for data quality validation rules, quarantine output, and quality reporting."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager
from src.validation.reporter import generate_consolidated_quality_report
from src.validation.rules import apply_validation_rules
from src.validation.validator import BatchValidator


def test_validation_rules_split_valid_and_rejected(sample_batch_df: pd.DataFrame) -> None:
    outcome = apply_validation_rules(sample_batch_df, batch_id="batch_dq_01")

    # 3 valid rows: normal row, cancellation row, guest row
    # 4 rejected rows: exact duplicate, zero price, neg qty without C, non-standard invoice + neg price
    assert outcome.metrics["total_rows"] == 7
    assert outcome.metrics["valid_rows"] == 3
    assert outcome.metrics["rejected_rows"] == 4

    reasons = set(outcome.rejected_df["rejection_reason"].tolist())
    assert "DQ_EXACT_DUPLICATE" in reasons
    assert "DQ_NON_POSITIVE_UNIT_PRICE" in reasons
    assert "DQ_NEGATIVE_QTY_WITHOUT_CANCEL" in reasons
    assert "DQ_NON_STANDARD_INVOICE" in reasons

    # Cancellations and missing CustomerID retained in Silver
    assert outcome.metrics["silver_profile"]["cancelled_transactions"] == 1
    assert outcome.metrics["silver_profile"]["missing_customer_id"] == 1


def test_batch_validator_writes_parquet_and_reports(test_config, sample_batch_csv: Path) -> None:
    lake = LakeManager(
        test_config.raw_zone,
        test_config.processed_zone,
        test_config.rejected_zone,
        test_config.curated_zone,
    )
    cp = CheckpointStore(test_config.checkpoint_db_path)
    cp.register_batch_start("batch_dq_02", str(sample_batch_csv), "abc123")

    validator = BatchValidator(lake, cp, test_config.reports_dir)
    outcome, valid_pq, rejected_pq = validator.validate_batch(sample_batch_csv, "batch_dq_02")

    assert valid_pq.exists()
    assert rejected_pq is not None and rejected_pq.exists()
    assert len(pd.read_parquet(valid_pq)) == 3
    assert len(pd.read_parquet(rejected_pq)) == 4

    consolidated = generate_consolidated_quality_report(test_config.reports_dir)
    assert consolidated["total_rows_processed"] == 7
    assert (test_config.reports_dir / "consolidated_data_quality_report.md").exists()
    dq_json = json.loads((test_config.reports_dir / "dq_batch_dq_02.json").read_text(encoding="utf-8"))
    assert dq_json["valid_rows"] == 3
