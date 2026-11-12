"""SQLite-backed batch tracking, watermarks, and idempotency checkpoint store."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class BatchRecord:
    batch_id: str
    source_file: str
    file_sha256: str
    batch_type: str
    status: str
    is_late_arriving: bool
    min_invoice_ts: str | None
    max_invoice_ts: str | None
    record_count_raw: int
    record_count_valid: int
    record_count_rejected: int
    record_count_curated: int
    record_count_warehouse: int
    attempt_count: int
    error_message: str | None
    created_at: str
    updated_at: str
    completed_at: str | None


class CheckpointStore:
    """Manages batch execution state, watermarks, and failure recovery checkpoints."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batch_registry (
                    batch_id TEXT PRIMARY KEY,
                    source_file TEXT NOT NULL,
                    file_sha256 TEXT NOT NULL,
                    batch_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    is_late_arriving INTEGER NOT NULL DEFAULT 0,
                    min_invoice_ts TEXT,
                    max_invoice_ts TEXT,
                    record_count_raw INTEGER NOT NULL DEFAULT 0,
                    record_count_valid INTEGER NOT NULL DEFAULT 0,
                    record_count_rejected INTEGER NOT NULL DEFAULT 0,
                    record_count_curated INTEGER NOT NULL DEFAULT 0,
                    record_count_warehouse INTEGER NOT NULL DEFAULT 0,
                    attempt_count INTEGER NOT NULL DEFAULT 1,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_batch_sha256
                ON batch_registry (file_sha256)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watermarks (
                    pipeline_name TEXT PRIMARY KEY,
                    high_watermark_ts TEXT,
                    last_batch_id TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS step_checkpoints (
                    batch_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (batch_id, step_name)
                )
                """
            )

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM batch_registry WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return self._row_to_batch(row) if row else None

    def get_completed_batch_by_sha256(self, file_sha256: str) -> BatchRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM batch_registry
                WHERE file_sha256 = ? AND status = 'LOADED'
                ORDER BY updated_at DESC LIMIT 1
                """,
                (file_sha256,),
            ).fetchone()
            return self._row_to_batch(row) if row else None

    def is_batch_completed(self, batch_id: str, file_sha256: str) -> bool:
        by_id = self.get_batch(batch_id)
        if by_id and by_id.status == "LOADED" and by_id.file_sha256 == file_sha256:
            return True
        by_hash = self.get_completed_batch_by_sha256(file_sha256)
        return by_hash is not None

    def register_batch_start(
        self,
        batch_id: str,
        source_file: str,
        file_sha256: str,
        batch_type: str = "incremental",
        is_late_arriving: bool = False,
    ) -> BatchRecord:
        now = _utc_now_iso()
        existing = self.get_batch(batch_id)
        with self._connect() as conn:
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO batch_registry (
                        batch_id, source_file, file_sha256, batch_type, status,
                        is_late_arriving, attempt_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'STARTED', ?, 1, ?, ?)
                    """,
                    (
                        batch_id,
                        source_file,
                        file_sha256,
                        batch_type,
                        int(is_late_arriving),
                        now,
                        now,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE batch_registry
                    SET source_file = ?,
                        file_sha256 = ?,
                        batch_type = ?,
                        status = 'STARTED',
                        is_late_arriving = ?,
                        attempt_count = attempt_count + 1,
                        error_message = NULL,
                        updated_at = ?
                    WHERE batch_id = ?
                    """,
                    (
                        source_file,
                        file_sha256,
                        batch_type,
                        int(is_late_arriving),
                        now,
                        batch_id,
                    ),
                )
        record = self.get_batch(batch_id)
        assert record is not None
        return record

    def update_batch_stage(
        self,
        batch_id: str,
        status: str,
        *,
        is_late_arriving: bool | None = None,
        min_invoice_ts: str | None = None,
        max_invoice_ts: str | None = None,
        record_count_raw: int | None = None,
        record_count_valid: int | None = None,
        record_count_rejected: int | None = None,
        record_count_curated: int | None = None,
        record_count_warehouse: int | None = None,
        error_message: str | None = None,
    ) -> None:
        now = _utc_now_iso()
        fields: list[str] = ["status = ?", "updated_at = ?"]
        values: list[object] = [status, now]

        optional_updates = {
            "is_late_arriving": int(is_late_arriving) if is_late_arriving is not None else None,
            "min_invoice_ts": min_invoice_ts,
            "max_invoice_ts": max_invoice_ts,
            "record_count_raw": record_count_raw,
            "record_count_valid": record_count_valid,
            "record_count_rejected": record_count_rejected,
            "record_count_curated": record_count_curated,
            "record_count_warehouse": record_count_warehouse,
            "error_message": error_message,
        }
        for col, val in optional_updates.items():
            if val is not None:
                fields.append(f"{col} = ?")
                values.append(val)

        if status == "LOADED":
            fields.append("completed_at = ?")
            values.append(now)

        values.append(batch_id)
        sql = f"UPDATE batch_registry SET {', '.join(fields)} WHERE batch_id = ?"
        with self._connect() as conn:
            conn.execute(sql, tuple(values))

    def record_step_checkpoint(
        self, batch_id: str, step_name: str, status: str, details_json: str | None = None
    ) -> None:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO step_checkpoints (batch_id, step_name, status, details_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(batch_id, step_name) DO UPDATE SET
                    status = excluded.status,
                    details_json = excluded.details_json,
                    updated_at = excluded.updated_at
                """,
                (batch_id, step_name, status, details_json, now),
            )

    def get_high_watermark(self, pipeline_name: str = "retail_sales") -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT high_watermark_ts FROM watermarks WHERE pipeline_name = ?",
                (pipeline_name,),
            ).fetchone()
            return row["high_watermark_ts"] if row else None

    def advance_high_watermark(
        self, candidate_ts: str, batch_id: str, pipeline_name: str = "retail_sales"
    ) -> str:
        """Advance high watermark monotonically; late-arriving batches never move it backward."""
        current = self.get_high_watermark(pipeline_name)
        new_watermark = candidate_ts if (current is None or candidate_ts > current) else current
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watermarks (pipeline_name, high_watermark_ts, last_batch_id, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(pipeline_name) DO UPDATE SET
                    high_watermark_ts = CASE
                        WHEN watermarks.high_watermark_ts IS NULL
                             OR excluded.high_watermark_ts > watermarks.high_watermark_ts
                        THEN excluded.high_watermark_ts
                        ELSE watermarks.high_watermark_ts
                    END,
                    last_batch_id = CASE
                        WHEN watermarks.high_watermark_ts IS NULL
                             OR excluded.high_watermark_ts > watermarks.high_watermark_ts
                        THEN excluded.last_batch_id
                        ELSE watermarks.last_batch_id
                    END,
                    updated_at = excluded.updated_at
                """,
                (pipeline_name, new_watermark, batch_id, now),
            )
        return new_watermark

    def list_batches(self) -> list[BatchRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM batch_registry ORDER BY created_at ASC, batch_id ASC"
            ).fetchall()
            return [self._row_to_batch(r) for r in rows]

    @staticmethod
    def _row_to_batch(row: sqlite3.Row) -> BatchRecord:
        return BatchRecord(
            batch_id=row["batch_id"],
            source_file=row["source_file"],
            file_sha256=row["file_sha256"],
            batch_type=row["batch_type"],
            status=row["status"],
            is_late_arriving=bool(row["is_late_arriving"]),
            min_invoice_ts=row["min_invoice_ts"],
            max_invoice_ts=row["max_invoice_ts"],
            record_count_raw=row["record_count_raw"],
            record_count_valid=row["record_count_valid"],
            record_count_rejected=row["record_count_rejected"],
            record_count_curated=row["record_count_curated"],
            record_count_warehouse=row["record_count_warehouse"],
            attempt_count=row["attempt_count"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )
