"""Batch file ingestion: schema verification, checksums, metadata, raw preservation, and duplicate detection."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.common.logging_utils import get_logger
from src.ingestion.downloader import compute_sha256
from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager

logger = get_logger("ingestion.ingester")


class SchemaValidationError(ValueError):
    """Raised when an incoming batch file fails structural or schema checks."""


@dataclass
class IngestionResult:
    batch_id: str
    source_file: str
    raw_file_path: Path
    metadata_path: Path
    file_sha256: str
    row_count: int
    min_invoice_ts: str | None
    max_invoice_ts: str | None
    is_late_arriving: bool
    skipped_as_duplicate: bool


class RawIngester:
    """Ingests batch files into the Bronze/Raw zone with strict validation and metadata."""

    def __init__(
        self,
        lake_manager: LakeManager,
        checkpoint_store: CheckpointStore,
        expected_columns: list[str],
    ) -> None:
        self.lake_manager = lake_manager
        self.checkpoint_store = checkpoint_store
        self.expected_columns = expected_columns

    def validate_file_and_schema(self, file_path: Path) -> pd.DataFrame:
        """Verify file exists, is non-empty, readable, and matches expected columns."""
        if not file_path.exists():
            raise FileNotFoundError(f"Batch file does not exist: {file_path}")
        if file_path.stat().st_size == 0:
            raise SchemaValidationError(f"Batch file is empty: {file_path}")

        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(
                file_path,
                dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str, "Country": str},
                keep_default_na=True,
            )
        elif suffix in {".xlsx", ".xls"}:
            df = pd.read_excel(
                file_path,
                dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str, "Country": str},
            )
        else:
            raise SchemaValidationError(f"Unsupported file format '{suffix}' for {file_path}")

        missing_cols = [c for c in self.expected_columns if c not in df.columns]
        if missing_cols:
            raise SchemaValidationError(
                f"Schema mismatch in {file_path.name}: missing columns {missing_cols}"
            )
        if df.empty:
            raise SchemaValidationError(f"Batch file contains 0 data rows: {file_path}")

        return df[self.expected_columns].copy()

    def ingest_batch(
        self,
        file_path: Path,
        batch_id: str,
        batch_type: str = "incremental",
        force: bool = False,
    ) -> IngestionResult:
        """
        Validate, fingerprint, and preserve a batch file into the raw zone.
        Detects already-processed identical batches via batch_id and SHA-256 checksum.
        """
        file_sha256 = compute_sha256(file_path)

        if not force and self.checkpoint_store.is_batch_completed(batch_id, file_sha256):
            existing = self.checkpoint_store.get_batch(batch_id) or self.checkpoint_store.get_completed_batch_by_sha256(file_sha256)
            raw_dir = self.lake_manager.raw_batch_dir(existing.batch_id if existing else batch_id)
            raw_file = raw_dir / file_path.name
            meta_file = raw_dir / "_ingestion_metadata.json"
            logger.info(
                "Skipping duplicate batch already loaded",
                extra={"batch_id": batch_id, "file_sha256": file_sha256},
            )
            return IngestionResult(
                batch_id=batch_id,
                source_file=str(file_path),
                raw_file_path=raw_file,
                metadata_path=meta_file,
                file_sha256=file_sha256,
                row_count=existing.record_count_raw if existing else 0,
                min_invoice_ts=existing.min_invoice_ts if existing else None,
                max_invoice_ts=existing.max_invoice_ts if existing else None,
                is_late_arriving=existing.is_late_arriving if existing else False,
                skipped_as_duplicate=True,
            )

        df = self.validate_file_and_schema(file_path)
        parsed_dates = pd.to_datetime(df["InvoiceDate"], errors="coerce").dropna()
        min_ts = parsed_dates.min().isoformat() if not parsed_dates.empty else None
        max_ts = parsed_dates.max().isoformat() if not parsed_dates.empty else None

        high_watermark = self.checkpoint_store.get_high_watermark()
        is_late = bool(
            batch_type == "late_arriving"
            or (high_watermark is not None and min_ts is not None and min_ts < high_watermark)
        )

        self.checkpoint_store.register_batch_start(
            batch_id=batch_id,
            source_file=str(file_path),
            file_sha256=file_sha256,
            batch_type=batch_type,
            is_late_arriving=is_late,
        )

        raw_dir = self.lake_manager.raw_batch_dir(batch_id)
        self.lake_manager.clean_batch_dir(raw_dir)
        raw_target = raw_dir / file_path.name
        shutil.copy2(file_path, raw_target)

        ingested_at = datetime.now(timezone.utc).isoformat()
        metadata: dict[str, Any] = {
            "batch_id": batch_id,
            "batch_type": batch_type,
            "simulation_mode": "historical_data_simulation",
            "source_file": str(file_path),
            "raw_file": str(raw_target),
            "file_sha256": file_sha256,
            "file_size_bytes": raw_target.stat().st_size,
            "row_count": int(len(df)),
            "columns": list(df.columns),
            "min_invoice_ts": min_ts,
            "max_invoice_ts": max_ts,
            "high_watermark_at_ingestion": high_watermark,
            "is_late_arriving": is_late,
            "ingested_at_utc": ingested_at,
        }
        metadata_path = self.lake_manager.write_json_metadata(
            raw_dir / "_ingestion_metadata.json", metadata
        )

        self.checkpoint_store.update_batch_stage(
            batch_id=batch_id,
            status="INGESTED",
            is_late_arriving=is_late,
            min_invoice_ts=min_ts,
            max_invoice_ts=max_ts,
            record_count_raw=int(len(df)),
        )
        self.checkpoint_store.record_step_checkpoint(batch_id, "ingestion", "COMPLETED")

        logger.info(
            "Batch ingested into raw zone",
            extra={
                "batch_id": batch_id,
                "row_count": len(df),
                "is_late_arriving": is_late,
                "min_invoice_ts": min_ts,
                "max_invoice_ts": max_ts,
            },
        )
        return IngestionResult(
            batch_id=batch_id,
            source_file=str(file_path),
            raw_file_path=raw_target,
            metadata_path=metadata_path,
            file_sha256=file_sha256,
            row_count=int(len(df)),
            min_invoice_ts=min_ts,
            max_invoice_ts=max_ts,
            is_late_arriving=is_late,
            skipped_as_duplicate=False,
        )
