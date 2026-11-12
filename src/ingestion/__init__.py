"""Data ingestion package exports."""

from src.ingestion.batch_generator import generate_historical_batches
from src.ingestion.downloader import compute_sha256, download_uci_online_retail
from src.ingestion.ingester import IngestionResult, RawIngester, SchemaValidationError

__all__ = [
    "compute_sha256",
    "download_uci_online_retail",
    "generate_historical_batches",
    "IngestionResult",
    "RawIngester",
    "SchemaValidationError",
]
