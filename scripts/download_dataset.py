"""Download the official UCI Online Retail dataset into data/source/."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import load_config
from src.common.logging_utils import configure_logging
from src.ingestion.downloader import download_uci_online_retail


def main() -> None:
    parser = argparse.ArgumentParser(description="Download official UCI Online Retail dataset.")
    parser.add_argument("--force", action="store_true", help="Re-download even if file exists.")
    args = parser.parse_args()

    cfg = load_config()
    configure_logging(level=cfg.env.log_level, log_format="console")
    path = download_uci_online_retail(
        source_dir=cfg.source_dir,
        url=cfg.yaml_config["dataset"]["url"],
        excel_filename=cfg.yaml_config["dataset"]["excel_filename"],
        force=args.force,
    )
    print(f"Dataset ready at: {path}")


if __name__ == "__main__":
    main()
