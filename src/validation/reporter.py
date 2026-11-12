"""Data quality reporting utilities generating per-batch and cumulative JSON/Markdown reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def generate_batch_quality_report(
    metrics: dict[str, Any],
    rejected_dir: Path,
    reports_dir: Path,
) -> Path:
    """Persist per-batch data quality summary JSON inside rejected zone and reports directory."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    batch_id = metrics["batch_id"]
    payload = {
        **metrics,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    for out_path in (
        rejected_dir / "_quality_summary.json",
        reports_dir / f"dq_{batch_id}.json",
    ):
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)

    return reports_dir / f"dq_{batch_id}.json"


def generate_consolidated_quality_report(
    reports_dir: Path,
    docs_path: Path | None = None,
) -> dict[str, Any]:
    """Aggregate all per-batch DQ JSON files into a consolidated JSON and Markdown report."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    batch_files = sorted(reports_dir.glob("dq_*.json"))

    total_rows = 0
    valid_rows = 0
    rejected_rows = 0
    rule_totals: dict[str, int] = {}
    primary_totals: dict[str, int] = {}
    silver_totals = {
        "cancelled_transactions": 0,
        "missing_customer_id": 0,
        "missing_description": 0,
        "unspecified_country": 0,
    }
    batches: list[dict[str, Any]] = []

    for bf in batch_files:
        with bf.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        batches.append(data)
        total_rows += int(data["total_rows"])
        valid_rows += int(data["valid_rows"])
        rejected_rows += int(data["rejected_rows"])
        for rule, cnt in data.get("rule_violation_counts", {}).items():
            rule_totals[rule] = rule_totals.get(rule, 0) + int(cnt)
        for reason, cnt in data.get("primary_rejection_counts", {}).items():
            primary_totals[reason] = primary_totals.get(reason, 0) + int(cnt)
        for k in silver_totals:
            silver_totals[k] += int(data.get("silver_profile", {}).get(k, 0))

    overall_pass_rate = round((valid_rows / total_rows * 100.0) if total_rows else 0.0, 4)
    consolidated = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_count": len(batches),
        "total_rows_processed": total_rows,
        "total_valid_rows": valid_rows,
        "total_rejected_rows": rejected_rows,
        "overall_pass_rate_pct": overall_pass_rate,
        "rule_violation_totals": dict(sorted(rule_totals.items(), key=lambda x: -x[1])),
        "primary_rejection_totals": dict(sorted(primary_totals.items(), key=lambda x: -x[1])),
        "silver_profile_totals": silver_totals,
        "batches": batches,
    }

    json_out = reports_dir / "consolidated_data_quality_report.json"
    with json_out.open("w", encoding="utf-8") as fh:
        json.dump(consolidated, fh, indent=2)

    md_lines = [
        "# Consolidated Data Quality Report",
        "",
        f"Generated (UTC): `{consolidated['generated_at_utc']}`",
        "",
        "## Overall Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Batches Validated | {consolidated['batch_count']:,} |",
        f"| Total Raw Rows | {total_rows:,} |",
        f"| Valid Rows (Silver) | {valid_rows:,} |",
        f"| Rejected Rows (Quarantine) | {rejected_rows:,} |",
        f"| Overall Pass Rate | {overall_pass_rate:.2f}% |",
        "",
        "## Primary Rejection Reasons",
        "",
        "| Rejection Code | Rejected Rows |",
        "| --- | ---: |",
    ]
    for reason, cnt in consolidated["primary_rejection_totals"].items():
        md_lines.append(f"| `{reason}` | {cnt:,} |")

    md_lines.extend(
        [
            "",
            "## All Rule Violations (Non-Exclusive)",
            "",
            "| Rule Code | Violation Count |",
            "| --- | ---: |",
        ]
    )
    for rule, cnt in consolidated["rule_violation_totals"].items():
        md_lines.append(f"| `{rule}` | {cnt:,} |")

    md_lines.extend(
        [
            "",
            "## Silver Zone Business Characteristics",
            "",
            "| Indicator | Valid Rows | Handling Strategy |",
            "| --- | ---: | --- |",
            f"| Cancelled Invoices (`C` prefix) | {silver_totals['cancelled_transactions']:,} | Retained with `is_cancelled = true` for net revenue & cancellation analytics |",
            f"| Missing `CustomerID` | {silver_totals['missing_customer_id']:,} | Retained with `is_guest_customer = true` mapped to `CustomerKey = -1` |",
            f"| Missing `Description` | {silver_totals['missing_description']:,} | Normalized to `'UNKNOWN PRODUCT'` with flag |",
            f"| `Unspecified` Country | {silver_totals['unspecified_country']:,} | Preserved in `dim_country` for full reconciliation |",
            "",
            "## Per-Batch Summary",
            "",
            "| Batch ID | Total | Valid | Rejected | Pass Rate |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for b in batches:
        md_lines.append(
            f"| `{b['batch_id']}` | {b['total_rows']:,} | {b['valid_rows']:,} | "
            f"{b['rejected_rows']:,} | {b['pass_rate_pct']:.2f}% |"
        )

    md_content = "\n".join(md_lines) + "\n"
    md_out = reports_dir / "consolidated_data_quality_report.md"
    md_out.write_text(md_content, encoding="utf-8")
    if docs_path is not None:
        docs_path.parent.mkdir(parents=True, exist_ok=True)
        docs_path.write_text(md_content, encoding="utf-8")

    return consolidated
