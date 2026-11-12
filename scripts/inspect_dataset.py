"""Inspect the downloaded UCI Online Retail dataset and print verified statistics."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import load_config


def inspect_excel(excel_path: Path) -> dict[str, object]:
    """Load the official Excel file and compute real data-quality statistics."""
    df = pd.read_excel(excel_path, dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str})

    total_rows = int(len(df))
    columns_info = {col: str(dtype) for col, dtype in df.dtypes.items()}
    missing_counts = {col: int(df[col].isna().sum()) for col in df.columns}
    missing_pct = {col: round(float(df[col].isna().mean() * 100.0), 4) for col in df.columns}

    exact_duplicates = int(df.duplicated(keep="first").sum())
    exact_duplicate_groups = int(df[df.duplicated(keep=False)].shape[0])

    invoice_str = df["InvoiceNo"].fillna("").astype(str).str.strip()
    cancellations = int(invoice_str.str.upper().str.startswith("C").sum())
    non_numeric_non_c_invoices = int(
        (~invoice_str.str.match(r"^[Cc]?\d+$")).sum()
    )

    qty_zero = int((df["Quantity"] == 0).sum())
    qty_negative = int((df["Quantity"] < 0).sum())
    qty_neg_non_cancel = int(((df["Quantity"] < 0) & (~invoice_str.str.upper().str.startswith("C"))).sum())

    price_zero = int((df["UnitPrice"] == 0).sum())
    price_negative = int((df["UnitPrice"] < 0).sum())
    price_non_positive = int((df["UnitPrice"] <= 0).sum())

    stock_str = df["StockCode"].fillna("").astype(str).str.strip().str.upper()
    special_codes = ["POST", "D", "M", "BANK CHARGES", "PADS", "DOT", "CRUK", "AMAZONFEE", "S"]
    special_code_counts = {
        code: int((stock_str == code).sum()) for code in special_codes if (stock_str == code).sum() > 0
    }

    unspecified_country = int((df["Country"].fillna("").str.strip() == "Unspecified").sum())
    unique_countries = int(df["Country"].dropna().nunique())
    unique_customers = int(df["CustomerID"].dropna().nunique())
    unique_invoices = int(df["InvoiceNo"].dropna().nunique())
    unique_stock_codes = int(df["StockCode"].dropna().nunique())

    min_date = str(pd.to_datetime(df["InvoiceDate"]).min())
    max_date = str(pd.to_datetime(df["InvoiceDate"]).max())

    return {
        "source_file": str(excel_path),
        "total_rows": total_rows,
        "columns": columns_info,
        "date_range": {"min": min_date, "max": max_date},
        "cardinality": {
            "unique_invoices": unique_invoices,
            "unique_stock_codes": unique_stock_codes,
            "unique_customers": unique_customers,
            "unique_countries": unique_countries,
        },
        "missing_values": missing_counts,
        "missing_percentages": missing_pct,
        "duplicates": {
            "exact_duplicate_excess_rows": exact_duplicates,
            "rows_involved_in_exact_duplicates": exact_duplicate_groups,
        },
        "anomalies": {
            "cancelled_invoices_prefix_C": cancellations,
            "non_standard_invoice_numbers": non_numeric_non_c_invoices,
            "quantity_zero": qty_zero,
            "quantity_negative_total": qty_negative,
            "quantity_negative_without_C_prefix": qty_neg_non_cancel,
            "unit_price_zero": price_zero,
            "unit_price_negative": price_negative,
            "unit_price_non_positive": price_non_positive,
            "unspecified_country_rows": unspecified_country,
            "special_non_product_stock_codes": special_code_counts,
        },
    }


def main() -> None:
    cfg = load_config()
    excel_path = cfg.source_dir / cfg.yaml_config["dataset"]["excel_filename"]
    if not excel_path.exists():
        raise FileNotFoundError(f"Dataset not found at {excel_path}. Run scripts/download_dataset.py first.")

    summary = inspect_excel(excel_path)
    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    out_file = cfg.reports_dir / "raw_dataset_profile.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
