"""End-to-end local pipeline orchestrator with checkpointing, recovery, and idempotency."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.common.config import AppConfig
from src.common.logging_utils import get_logger
from src.ingestion.ingester import RawIngester
from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager
from src.transformation.etl_job import SparkRetailETL
from src.transformation.spark_session import create_spark_session
from src.validation.reporter import generate_consolidated_quality_report
from src.validation.validator import BatchValidator
from src.warehouse.loader import DuckDBWarehouseLoader

logger = get_logger("orchestration.pipeline")


@dataclass
class BatchExecutionSummary:
    batch_id: str
    batch_type: str
    status: str
    skipped_as_duplicate: bool
    is_late_arriving: bool
    raw_rows: int
    valid_rows: int
    rejected_rows: int
    curated_rows: int
    warehouse_total_rows: int
    duration_seconds: float


class RetailPipelineOrchestrator:
    """Coordinates Ingestion -> Validation -> PySpark ETL -> DuckDB Warehouse Load."""

    def __init__(self, config: AppConfig, spark: SparkSession | None = None) -> None:
        self.config = config
        self.checkpoint_store = CheckpointStore(config.checkpoint_db_path)
        self.lake_manager = LakeManager(
            raw_dir=config.raw_zone,
            processed_dir=config.processed_zone,
            rejected_dir=config.rejected_zone,
            curated_dir=config.curated_zone,
        )
        self.ingester = RawIngester(
            lake_manager=self.lake_manager,
            checkpoint_store=self.checkpoint_store,
            expected_columns=config.yaml_config["dataset"]["expected_columns"],
        )
        self.validator = BatchValidator(
            lake_manager=self.lake_manager,
            checkpoint_store=self.checkpoint_store,
            reports_dir=config.reports_dir,
            deduplicate_exact_rows=bool(config.yaml_config["validation"]["deduplicate_exact_rows"]),
        )
        self._owns_spark = spark is None
        self.spark = spark or create_spark_session(
            project_root=config.project_root,
            app_name=config.yaml_config["spark"]["app_name"],
            master=config.env.spark_master,
            driver_memory=config.env.spark_driver_memory,
            shuffle_partitions=config.env.spark_sql_shuffle_partitions,
            timezone=config.env.spark_timezone,
        )
        self.spark_etl = SparkRetailETL(
            spark=self.spark,
            lake_manager=self.lake_manager,
            checkpoint_store=self.checkpoint_store,
            special_non_product_codes=config.yaml_config["validation"][
                "special_non_product_stock_codes"
            ],
        )
        self.warehouse_loader = DuckDBWarehouseLoader(
            db_path=config.warehouse_db_path,
            ddl_path=config.sql_dir / "ddl" / "01_warehouse_schema.sql",
            checkpoint_store=self.checkpoint_store,
        )

    def close(self) -> None:
        if self._owns_spark and self.spark is not None:
            self.spark.stop()

    def process_batch(
        self,
        file_path: Path,
        batch_id: str,
        batch_type: str = "incremental",
        force: bool = False,
        rebuild_aggregates: bool = True,
    ) -> BatchExecutionSummary:
        """Execute all pipeline stages for a single batch file."""
        started = time.perf_counter()
        try:
            ingest_res = self.ingester.ingest_batch(
                file_path=file_path,
                batch_id=batch_id,
                batch_type=batch_type,
                force=force,
            )
            if ingest_res.skipped_as_duplicate:
                existing = self.checkpoint_store.get_batch(batch_id)
                elapsed = round(time.perf_counter() - started, 3)
                return BatchExecutionSummary(
                    batch_id=batch_id,
                    batch_type=batch_type,
                    status="SKIPPED_DUPLICATE",
                    skipped_as_duplicate=True,
                    is_late_arriving=ingest_res.is_late_arriving,
                    raw_rows=existing.record_count_raw if existing else 0,
                    valid_rows=existing.record_count_valid if existing else 0,
                    rejected_rows=existing.record_count_rejected if existing else 0,
                    curated_rows=existing.record_count_curated if existing else 0,
                    warehouse_total_rows=0,
                    duration_seconds=elapsed,
                )

            outcome, valid_parquet, _ = self.validator.validate_batch(
                raw_file=ingest_res.raw_file_path,
                batch_id=batch_id,
            )
            etl_res = self.spark_etl.run_batch(
                valid_parquet_path=valid_parquet,
                batch_id=batch_id,
                build_aggregates=rebuild_aggregates,
            )
            wh_res = self.warehouse_loader.load_batch(
                curated_transactions_dir=etl_res.curated_transactions_path,
                batch_id=batch_id,
                batch_type=batch_type,
                is_late_arriving=ingest_res.is_late_arriving,
            )

            elapsed = round(time.perf_counter() - started, 3)
            return BatchExecutionSummary(
                batch_id=batch_id,
                batch_type=batch_type,
                status="LOADED",
                skipped_as_duplicate=False,
                is_late_arriving=ingest_res.is_late_arriving,
                raw_rows=ingest_res.row_count,
                valid_rows=outcome.metrics["valid_rows"],
                rejected_rows=outcome.metrics["rejected_rows"],
                curated_rows=etl_res.curated_row_count,
                warehouse_total_rows=wh_res.fact_total_rows,
                duration_seconds=elapsed,
            )
        except Exception as exc:
            self.checkpoint_store.update_batch_stage(
                batch_id=batch_id,
                status="FAILED",
                error_message=str(exc),
            )
            logger.exception("Batch execution failed", extra={"batch_id": batch_id, "error": str(exc)})
            raise

    def run_manifest(
        self,
        manifest_path: Path | None = None,
        *,
        include_late_arriving: bool = True,
        force: bool = False,
        only_batch_types: set[str] | None = None,
    ) -> dict[str, Any]:
        """Run batches listed in batch_manifest.json with incremental tracking."""
        m_path = manifest_path or (self.config.batches_dir / "batch_manifest.json")
        if not m_path.exists():
            raise FileNotFoundError(f"Batch manifest not found at {m_path}. Run scripts/prepare_batches.py first.")

        with m_path.open("r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        batch_entries = manifest["batches"]
        if only_batch_types:
            batch_entries = [b for b in batch_entries if b["batch_type"] in only_batch_types]
        if not include_late_arriving:
            batch_entries = [b for b in batch_entries if b["batch_type"] != "late_arriving"]

        summaries: list[BatchExecutionSummary] = []
        any_new_data = False
        for entry in batch_entries:
            summary = self.process_batch(
                file_path=Path(entry["file_path"]),
                batch_id=entry["batch_id"],
                batch_type=entry["batch_type"],
                force=force,
                rebuild_aggregates=False,
            )
            summaries.append(summary)
            if not summary.skipped_as_duplicate:
                any_new_data = True

        if any_new_data:
            monthly_cnt, product_cnt, customer_cnt = self.spark_etl.rebuild_gold_aggregates()
        else:
            monthly_cnt, product_cnt, customer_cnt = (0, 0, 0)

        dq_summary = generate_consolidated_quality_report(
            reports_dir=self.config.reports_dir,
            docs_path=self.config.project_root / "docs" / "data_quality_report.md",
        )

        run_summary: dict[str, Any] = {
            "executed_at_utc": datetime.now(timezone.utc).isoformat(),
            "high_watermark_ts": self.checkpoint_store.get_high_watermark(),
            "batches_attempted": len(summaries),
            "batches_loaded": sum(1 for s in summaries if s.status == "LOADED"),
            "batches_skipped_duplicate": sum(1 for s in summaries if s.skipped_as_duplicate),
            "gold_aggregates": {
                "monthly_country_rows": monthly_cnt,
                "product_performance_rows": product_cnt,
                "customer_summary_rows": customer_cnt,
            },
            "data_quality_overview": {
                "total_rows_processed": dq_summary["total_rows_processed"],
                "total_valid_rows": dq_summary["total_valid_rows"],
                "total_rejected_rows": dq_summary["total_rejected_rows"],
                "overall_pass_rate_pct": dq_summary["overall_pass_rate_pct"],
            },
            "batch_results": [s.__dict__ for s in summaries],
        }

        self.lake_manager.write_json_metadata(
            self.config.reports_dir / "pipeline_run_summary.json", run_summary
        )
        return run_summary
