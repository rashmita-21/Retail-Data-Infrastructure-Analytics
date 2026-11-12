"""Tests for raw batch ingestion, schema validation, and duplicate file detection."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.ingester import RawIngester, SchemaValidationError
from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager


def _make_ingester(test_config) -> RawIngester:
    lake = LakeManager(
        test_config.raw_zone,
        test_config.processed_zone,
        test_config.rejected_zone,
        test_config.curated_zone,
    )
    cp = CheckpointStore(test_config.checkpoint_db_path)
    return RawIngester(
        lake_manager=lake,
        checkpoint_store=cp,
        expected_columns=test_config.yaml_config["dataset"]["expected_columns"],
    )


def test_ingest_valid_batch_creates_raw_and_metadata(test_config, sample_batch_csv: Path) -> None:
    ingester = _make_ingester(test_config)
    res = ingester.ingest_batch(sample_batch_csv, batch_id="batch_test_01", batch_type="incremental")

    assert not res.skipped_as_duplicate
    assert res.row_count == 7
    assert res.raw_file_path.exists()
    assert res.metadata_path.exists()

    meta = json.loads(res.metadata_path.read_text(encoding="utf-8"))
    assert meta["batch_id"] == "batch_test_01"
    assert meta["file_sha256"] == res.file_sha256
    assert meta["row_count"] == 7


def test_ingest_rejects_missing_columns(test_config, tmp_path: Path) -> None:
    ingester = _make_ingester(test_config)
    bad_csv = tmp_path / "bad_schema.csv"
    pd.DataFrame([{"InvoiceNo": "123", "Quantity": 1}]).to_csv(bad_csv, index=False)

    with pytest.raises(SchemaValidationError, match="missing columns"):
        ingester.ingest_batch(bad_csv, batch_id="batch_bad_schema")


def test_ingest_rejects_empty_file(test_config, tmp_path: Path) -> None:
    ingester = _make_ingester(test_config)
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("", encoding="utf-8")

    with pytest.raises(SchemaValidationError, match="empty"):
        ingester.ingest_batch(empty_file, batch_id="batch_empty")


def test_ingest_skips_completed_duplicate_sha256(test_config, sample_batch_csv: Path) -> None:
    ingester = _make_ingester(test_config)
    first = ingester.ingest_batch(sample_batch_csv, batch_id="batch_dup_01")
    ingester.checkpoint_store.update_batch_stage("batch_dup_01", status="LOADED")

    second = ingester.ingest_batch(sample_batch_csv, batch_id="batch_dup_01")
    assert second.skipped_as_duplicate
    assert second.file_sha256 == first.file_sha256
