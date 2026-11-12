"""Dataset download and integrity verification for the UCI Online Retail dataset."""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import requests

from src.common.logging_utils import get_logger
from src.common.retry import retry_on_transient_error

logger = get_logger("ingestion.downloader")

UCI_DEFAULT_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"


def compute_sha256(file_path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 checksum of a file."""
    digest = hashlib.sha256()
    with file_path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


@retry_on_transient_error(max_attempts=3, backoff_seconds=2.0)
def _download_file(url: str, destination: Path, timeout: int = 120) -> None:
    logger.info("Downloading dataset", extra={"url": url, "destination": str(destination)})
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        with tmp_path.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
        tmp_path.replace(destination)


def download_uci_online_retail(
    source_dir: Path,
    url: str = UCI_DEFAULT_URL,
    excel_filename: str = "Online Retail.xlsx",
    force: bool = False,
) -> Path:
    """Download and extract the official UCI Online Retail Excel dataset."""
    source_dir.mkdir(parents=True, exist_ok=True)
    target_excel = source_dir / excel_filename
    zip_path = source_dir / "online_retail.zip"

    if target_excel.exists() and not force:
        logger.info(
            "Dataset already present locally; skipping download",
            extra={"path": str(target_excel), "sha256": compute_sha256(target_excel)},
        )
        return target_excel

    _download_file(url, zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        xlsx_members = [m for m in members if m.lower().endswith(".xlsx")]
        if not xlsx_members:
            raise FileNotFoundError(f"No .xlsx file found inside archive {zip_path}: {members}")
        zf.extract(xlsx_members[0], path=source_dir)
        extracted = source_dir / xlsx_members[0]
        if extracted != target_excel:
            extracted.replace(target_excel)

    logger.info(
        "Dataset downloaded and extracted",
        extra={
            "path": str(target_excel),
            "size_bytes": target_excel.stat().st_size,
            "sha256": compute_sha256(target_excel),
        },
    )
    return target_excel
