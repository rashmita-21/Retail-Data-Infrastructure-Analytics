"""Generate reproducible historical daily simulation batches + late-arriving test batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import load_config
from src.common.logging_utils import configure_logging
from src.ingestion.batch_generator import generate_historical_batches


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate historical-data simulation batches from the UCI Online Retail dataset."
    )
    parser.add_argument("--force", action="store_true", help="Regenerate batches even if manifest exists.")
    args = parser.parse_args()

    cfg = load_config()
    configure_logging(level=cfg.env.log_level, log_format="console")
    excel_path = cfg.source_dir / cfg.yaml_config["dataset"]["excel_filename"]
    if not excel_path.exists():
        raise FileNotFoundError(f"Source Excel not found at {excel_path}. Run scripts/download_dataset.py first.")

    sim_cfg = cfg.yaml_config["batch_simulation"]
    manifest = generate_historical_batches(
        excel_path=excel_path,
        batches_dir=cfg.batches_dir,
        incremental_days=int(sim_cfg["incremental_batch_days"]),
        late_arrival_sample_size=int(sim_cfg["late_arrival_sample_size"]),
        random_seed=int(sim_cfg["random_seed"]),
        force=args.force,
    )
    print(
        json.dumps(
            {
                "simulation_mode": manifest["simulation_mode"],
                "total_source_rows": manifest["total_source_rows"],
                "total_batched_rows": manifest["total_batched_rows"],
                "batch_count": len(manifest["batches"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
