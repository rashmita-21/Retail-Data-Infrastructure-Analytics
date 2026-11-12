"""Structured logging setup supporting JSON and human-readable console formats."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON payloads for log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            payload.update(record.extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ContextLogger(logging.LoggerAdapter):
    """LoggerAdapter that attaches structured context fields to every log event."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = kwargs.get("extra", {})
        merged = {**(self.extra or {}), **extra}
        kwargs["extra"] = {"extra_fields": merged}
        return msg, kwargs


def configure_logging(
    level: str = "INFO",
    log_format: str = "json",
    log_file: str | None = None,
) -> None:
    """Configure root logger with structured formatting."""
    root = logging.getLogger("retail_platform")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    if log_format.lower() == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)

    root.propagate = False


def get_logger(name: str, **context: Any) -> ContextLogger:
    """Return a structured logger bound to optional context metadata."""
    base_logger = logging.getLogger(f"retail_platform.{name}")
    if not logging.getLogger("retail_platform").handlers:
        configure_logging()
    return ContextLogger(base_logger, context)
