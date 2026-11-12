"""Retry and resilience utilities using exponential backoff."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

T = TypeVar("T")


def retry_on_transient_error(
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (OSError, ConnectionError, TimeoutError),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator applying exponential backoff on transient I/O or network failures."""

    return retry(
        reraise=True,
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=backoff_seconds, min=1, max=30),
        retry=retry_if_exception_type(exceptions),
    )
