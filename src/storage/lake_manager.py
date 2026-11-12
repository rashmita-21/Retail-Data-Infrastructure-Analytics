"""Data lake zone manager for raw, processed, rejected, and curated layers."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any


class LakeManager:
    """Manages local Medallion-style data lake zones with idempotent batch paths."""

    def __init__(
        self,
        raw_dir: Path,
        processed_dir: Path,
        rejected_dir: Path,
        curated_dir: Path,
    ) -> None:
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.rejected_dir = rejected_dir
        self.curated_dir = curated_dir
        for zone in (self.raw_dir, self.processed_dir, self.rejected_dir, self.curated_dir):
            zone.mkdir(parents=True, exist_ok=True)

    def raw_batch_dir(self, batch_id: str) -> Path:
        path = self.raw_dir / f"batch_id={batch_id}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def processed_batch_dir(self, batch_id: str) -> Path:
        path = self.processed_dir / f"batch_id={batch_id}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def rejected_batch_dir(self, batch_id: str) -> Path:
        path = self.rejected_dir / f"batch_id={batch_id}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def curated_transactions_dir(self) -> Path:
        path = self.curated_dir / "transactions_partitioned"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def curated_aggregates_dir(self, dataset_name: str) -> Path:
        path = self.curated_dir / dataset_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_json_metadata(self, target_path: Path, payload: dict[str, Any]) -> Path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        tmp_path.replace(target_path)
        return target_path

    def clean_batch_dir(self, target_dir: Path) -> None:
        """Remove prior partial outputs for a batch before idempotent rewrite."""
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
