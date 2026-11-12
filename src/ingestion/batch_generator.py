"""Reproducible historical-data batch generator and late-arriving batch simulator."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.common.logging_utils import get_logger

logger = get_logger("ingestion.batch_generator")


@dataclass
class BatchManifestEntry:
    batch_id: str
    file_path: str
    batch_type: str
    row_count: int
    min_invoice_date: str
    max_invoice_date: str
    simulation_mode: str = "historical_data_simulation"


def generate_historical_batches(
    excel_path: Path,
    batches_dir: Path,
    *,
    incremental_days: int = 14,
    late_arrival_sample_size: int = 250,
    random_seed: int = 42,
    force: bool = False,
) -> dict[str, Any]:
    """
    Split the official UCI Online Retail dataset into reproducible chronological batches
    using historical InvoiceDate values (clearly marked as historical-data simulation).

    Layout created in `batches_dir`:
      - batch_0001_historical_backfill.csv   : all dates prior to the incremental window
      - batch_0002_daily_YYYY-MM-DD.csv ...  : final `incremental_days` distinct invoice dates
      - batch_9999_late_arriving.csv         : deterministic held-back slice from earlier dates
      - batch_manifest.json                  : full provenance & simulation metadata
    """
    batches_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = batches_dir / "batch_manifest.json"
    if manifest_path.exists() and not force:
        with manifest_path.open("r", encoding="utf-8") as fh:
            existing = json.load(fh)
        logger.info(
            "Batch manifest already exists; skipping regeneration",
            extra={"manifest_path": str(manifest_path), "batch_count": len(existing.get("batches", []))},
        )
        return existing

    # Clear existing CSVs if regenerating
    for old_file in batches_dir.glob("*.csv"):
        old_file.unlink()

    logger.info("Loading Excel dataset for batch generation", extra={"excel_path": str(excel_path)})
    df = pd.read_excel(
        excel_path,
        dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str},
    )
    df["_source_row_num"] = range(1, len(df) + 1)
    df["_invoice_dt"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["_invoice_day"] = df["_invoice_dt"].dt.strftime("%Y-%m-%d")

    distinct_days = sorted(df["_invoice_day"].dropna().unique().tolist())
    if len(distinct_days) <= incremental_days + 2:
        raise ValueError(
            f"Not enough distinct days ({len(distinct_days)}) for incremental_days={incremental_days}"
        )

    incremental_day_list = distinct_days[-incremental_days:]
    backfill_cutoff_day = incremental_day_list[0]

    # Select a deterministic late-arriving slice from mid-year history (strictly before incremental window)
    mid_history_mask = (df["_invoice_day"] < backfill_cutoff_day) & (
        df["_invoice_day"] >= distinct_days[len(distinct_days) // 3]
    )
    mid_candidates = df.loc[mid_history_mask]
    actual_late_size = min(late_arrival_sample_size, len(mid_candidates))
    late_df = mid_candidates.sample(n=actual_late_size, random_state=random_seed).sort_values(
        "_source_row_num"
    )
    late_indices = set(late_df.index.tolist())

    remaining_df = df.loc[~df.index.isin(late_indices)].copy()
    backfill_df = remaining_df.loc[remaining_df["_invoice_day"] < backfill_cutoff_day].sort_values(
        "_source_row_num"
    )

    export_cols = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    entries: list[BatchManifestEntry] = []

    # 1. Historical backfill batch
    backfill_id = "batch_0001_historical_backfill"
    backfill_path = batches_dir / f"{backfill_id}.csv"
    backfill_df[export_cols].to_csv(backfill_path, index=False)
    entries.append(
        BatchManifestEntry(
            batch_id=backfill_id,
            file_path=str(backfill_path),
            batch_type="backfill",
            row_count=int(len(backfill_df)),
            min_invoice_date=str(backfill_df["_invoice_dt"].min()),
            max_invoice_date=str(backfill_df["_invoice_dt"].max()),
        )
    )

    # 2. Daily incremental batches (historical-data simulation)
    for idx, day_str in enumerate(incremental_day_list, start=2):
        day_df = remaining_df.loc[remaining_df["_invoice_day"] == day_str].sort_values(
            "_source_row_num"
        )
        if day_df.empty:
            continue
        batch_id = f"batch_{idx:04d}_daily_{day_str}"
        batch_path = batches_dir / f"{batch_id}.csv"
        day_df[export_cols].to_csv(batch_path, index=False)
        entries.append(
            BatchManifestEntry(
                batch_id=batch_id,
                file_path=str(batch_path),
                batch_type="incremental",
                row_count=int(len(day_df)),
                min_invoice_date=str(day_df["_invoice_dt"].min()),
                max_invoice_date=str(day_df["_invoice_dt"].max()),
            )
        )

    # 3. Late-arriving batch (held-back historical records delivered out-of-order)
    late_id = "batch_9999_late_arriving"
    late_path = batches_dir / f"{late_id}.csv"
    late_df[export_cols].to_csv(late_path, index=False)
    entries.append(
        BatchManifestEntry(
            batch_id=late_id,
            file_path=str(late_path),
            batch_type="late_arriving",
            row_count=int(len(late_df)),
            min_invoice_date=str(late_df["_invoice_dt"].min()),
            max_invoice_date=str(late_df["_invoice_dt"].max()),
        )
    )

    manifest = {
        "simulation_mode": "historical_data_simulation",
        "note": (
            "Daily incremental batches and the late-arriving batch are simulated by slicing "
            "the historical UCI Online Retail dataset along InvoiceDate without altering or fabricating records."
        ),
        "source_excel": str(excel_path),
        "total_source_rows": int(len(df)),
        "total_batched_rows": sum(e.row_count for e in entries),
        "backfill_cutoff_day": backfill_cutoff_day,
        "incremental_days_count": len(incremental_day_list),
        "late_arrival_sample_size": actual_late_size,
        "random_seed": random_seed,
        "batches": [e.__dict__ for e in entries],
    }

    with manifest_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    logger.info(
        "Generated historical simulation batches",
        extra={
            "total_source_rows": manifest["total_source_rows"],
            "total_batched_rows": manifest["total_batched_rows"],
            "batch_count": len(entries),
        },
    )
    return manifest
