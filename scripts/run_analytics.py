"""Execute all analytical SQL queries against the DuckDB warehouse and export verified results."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import load_config


def run_all_queries(db_path: Path, analytics_dir: Path, reports_dir: Path) -> dict[str, object]:
    """Execute every .sql file in sql/analytics/ and save summary + CSV previews."""
    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse database not found at {db_path}. Run pipeline first.")

    out_csv_dir = reports_dir / "sql_outputs"
    out_csv_dir.mkdir(parents=True, exist_ok=True)

    sql_files = sorted(analytics_dir.glob("*.sql"))
    results_summary: list[dict[str, object]] = []

    with duckdb.connect(str(db_path), read_only=True) as conn:
        for sql_file in sql_files:
            query_text = sql_file.read_text(encoding="utf-8")
            rel = conn.sql(query_text)
            df = rel.df()
            csv_path = out_csv_dir / f"{sql_file.stem}.csv"
            df.to_csv(csv_path, index=False)
            results_summary.append(
                {
                    "query_file": sql_file.name,
                    "row_count": int(len(df)),
                    "columns": list(df.columns),
                    "preview_top_3": df.head(3).to_dict(orient="records"),
                }
            )

    summary_payload = {
        "database_path": str(db_path),
        "queries_executed": len(results_summary),
        "results": results_summary,
    }
    summary_file = reports_dir / "analytics_execution_summary.json"
    with summary_file.open("w", encoding="utf-8") as fh:
        json.dump(summary_payload, fh, indent=2, default=str)

    return summary_payload


def main() -> None:
    cfg = load_config()
    summary = run_all_queries(
        db_path=cfg.warehouse_db_path,
        analytics_dir=cfg.sql_dir / "analytics",
        reports_dir=cfg.reports_dir,
    )
    print(f"Executed {summary['queries_executed']} analytical SQL queries successfully.")
    for item in summary["results"]:
        print(f"  - {item['query_file']}: {item['row_count']} rows")


if __name__ == "__main__":
    main()
