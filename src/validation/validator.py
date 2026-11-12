"""Batch validator writing validated Silver records and quarantined Rejected records."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.common.logging_utils import get_logger
from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager
from src.validation.reporter import generate_batch_quality_report
from src.validation.rules import ValidationOutcome, apply_validation_rules

logger = get_logger("validation.validator")


class BatchValidator:
    """Validates raw batch files and writes Silver (processed) and Rejected Parquet zones."""

    def __init__(
        self,
        lake_manager: LakeManager,
        checkpoint_store: CheckpointStore,
        reports_dir: Path,
        deduplicate_exact_rows: bool = True,
    ) -> None:
        self.lake_manager = lake_manager
        self.checkpoint_store = checkpoint_store
        self.reports_dir = reports_dir
        self.deduplicate_exact_rows = deduplicate_exact_rows

    def validate_batch(self, raw_file: Path, batch_id: str) -> tuple[ValidationOutcome, Path, Path | None]:
        """Run validation rules on a raw batch file and persist Parquet outputs."""
        df = pd.read_csv(
            raw_file,
            dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str, "Country": str},
        )
        outcome = apply_validation_rules(
            df,
            batch_id=batch_id,
            deduplicate_exact_rows=self.deduplicate_exact_rows,
        )

        processed_dir = self.lake_manager.processed_batch_dir(batch_id)
        self.lake_manager.clean_batch_dir(processed_dir)
        valid_parquet = processed_dir / "valid_records.parquet"
        self._normalize_for_parquet(outcome.valid_df).to_parquet(
            valid_parquet, index=False, engine="pyarrow"
        )

        rejected_dir = self.lake_manager.rejected_batch_dir(batch_id)
        self.lake_manager.clean_batch_dir(rejected_dir)
        rejected_parquet: Path | None = None
        if not outcome.rejected_df.empty:
            rejected_parquet = rejected_dir / "rejected_records.parquet"
            self._normalize_for_parquet(outcome.rejected_df, include_rejection=True).to_parquet(
                rejected_parquet, index=False, engine="pyarrow"
            )

        generate_batch_quality_report(
            metrics=outcome.metrics,
            rejected_dir=rejected_dir,
            reports_dir=self.reports_dir,
        )

        self.checkpoint_store.update_batch_stage(
            batch_id=batch_id,
            status="VALIDATED",
            record_count_valid=outcome.metrics["valid_rows"],
            record_count_rejected=outcome.metrics["rejected_rows"],
        )
        self.checkpoint_store.record_step_checkpoint(batch_id, "validation", "COMPLETED")

        logger.info(
            "Batch validation completed",
            extra={
                "batch_id": batch_id,
                "valid_rows": outcome.metrics["valid_rows"],
                "rejected_rows": outcome.metrics["rejected_rows"],
                "pass_rate_pct": outcome.metrics["pass_rate_pct"],
            },
        )
        return outcome, valid_parquet, rejected_parquet

    @staticmethod
    def _normalize_for_parquet(df: pd.DataFrame, include_rejection: bool = False) -> pd.DataFrame:
        out = df.copy()
        for col in ("InvoiceNo", "StockCode", "Description", "CustomerID", "Country", "batch_id"):
            if col in out.columns:
                out[col] = out[col].where(out[col].notna(), None).astype("string")
        out["InvoiceDate"] = out["InvoiceDate"].astype("string")
        out["Quantity"] = pd.to_numeric(out["Quantity"], errors="coerce").astype("Int64")
        out["UnitPrice"] = pd.to_numeric(out["UnitPrice"], errors="coerce").astype("float64")
        if include_rejection:
            out["rejection_reason"] = out["rejection_reason"].astype("string")
            out["rejection_rules_all"] = out["rejection_rules_all"].astype("string")
        return out
