"""CLI entrypoint for executing the end-to-end Retail Data Infrastructure pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import load_config
from src.common.logging_utils import configure_logging
from src.ingestion.batch_generator import generate_historical_batches
from src.ingestion.downloader import download_uci_online_retail
from src.orchestration.pipeline import RetailPipelineOrchestrator


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Retail Data Infrastructure & Analytics Platform pipeline."
    )
    parser.add_argument(
        "--mode",
        choices=["all", "backfill", "incremental", "late-arriving", "batch"],
        default="all",
        help="Execution scope: all batches, backfill only, incremental only, late-arriving only, or single batch file.",
    )
    parser.add_argument("--batch-file", type=Path, help="Path to a single batch CSV (when --mode batch).")
    parser.add_argument("--batch-id", type=str, help="Explicit batch_id (when --mode batch).")
    parser.add_argument("--force", action="store_true", help="Force re-execution of already-loaded batches.")
    parser.add_argument(
        "--prepare-data",
        action="store_true",
        help="Download UCI dataset and generate historical batches if not already prepared.",
    )
    parser.add_argument(
        "--log-format",
        choices=["console", "json"],
        default="console",
        help="Logging output format.",
    )
    args = parser.parse_args()

    cfg = load_config()
    configure_logging(level=cfg.env.log_level, log_format=args.log_format)

    if args.prepare_data:
        excel_path = download_uci_online_retail(
            source_dir=cfg.source_dir,
            url=cfg.yaml_config["dataset"]["url"],
            excel_filename=cfg.yaml_config["dataset"]["excel_filename"],
        )
        sim_cfg = cfg.yaml_config["batch_simulation"]
        generate_historical_batches(
            excel_path=excel_path,
            batches_dir=cfg.batches_dir,
            incremental_days=int(sim_cfg["incremental_batch_days"]),
            late_arrival_sample_size=int(sim_cfg["late_arrival_sample_size"]),
            random_seed=int(sim_cfg["random_seed"]),
        )

    orchestrator = RetailPipelineOrchestrator(cfg)
    try:
        if args.mode == "batch":
            if not args.batch_file or not args.batch_id:
                parser.error("--batch-file and --batch-id are required when --mode batch")
            res = orchestrator.process_batch(
                file_path=args.batch_file,
                batch_id=args.batch_id,
                batch_type="manual",
                force=args.force,
            )
            print(json.dumps(res.__dict__, indent=2))
            return

        type_map = {
            "all": None,
            "backfill": {"backfill"},
            "incremental": {"incremental"},
            "late-arriving": {"late_arriving"},
        }
        summary = orchestrator.run_manifest(
            include_late_arriving=(args.mode in {"all", "late-arriving"}),
            force=args.force,
            only_batch_types=type_map[args.mode],
        )
        print(json.dumps(summary, indent=2))
    finally:
        orchestrator.close()


if __name__ == "__main__":
    main()
