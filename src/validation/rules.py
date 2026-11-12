"""Data quality validation rules for retail transactions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ValidationOutcome:
    valid_df: pd.DataFrame
    rejected_df: pd.DataFrame
    metrics: dict[str, Any]


def apply_validation_rules(
    df: pd.DataFrame,
    batch_id: str,
    *,
    deduplicate_exact_rows: bool = True,
) -> ValidationOutcome:
    """
    Validate raw retail batch rows and partition into valid (Silver) vs rejected (Quarantine).

    Tracks both primary rejection reason and all violated rule codes per rejected row.
    """
    work = df.copy()
    work["_source_row_index"] = range(1, len(work) + 1)

    # Normalize string columns safely without destroying raw values for rejected inspection
    inv_str = work["InvoiceNo"].fillna("").astype(str).str.strip()
    stock_str = work["StockCode"].fillna("").astype(str).str.strip()
    country_str = work["Country"].fillna("").astype(str).str.strip()
    desc_str = work["Description"].fillna("").astype(str).str.strip()
    cust_str = work["CustomerID"].where(work["CustomerID"].notna(), None)

    parsed_ts = pd.to_datetime(work["InvoiceDate"], errors="coerce")
    parsed_qty = pd.to_numeric(work["Quantity"], errors="coerce")
    parsed_price = pd.to_numeric(work["UnitPrice"], errors="coerce")

    # Individual rule masks (True = VIOLATION)
    v_missing_required = (
        (inv_str == "")
        | (stock_str == "")
        | (country_str == "")
        | parsed_ts.isna()
        | parsed_qty.isna()
        | parsed_price.isna()
    )
    v_non_int_qty = parsed_qty.notna() & ((parsed_qty % 1) != 0)
    v_non_standard_invoice = (inv_str != "") & (~inv_str.str.match(r"^[Cc]?\d+$"))
    v_zero_qty = parsed_qty.notna() & (parsed_qty == 0)
    v_non_positive_price = parsed_price.notna() & (parsed_price <= 0)

    is_cancel = inv_str.str.upper().str.startswith("C")
    v_neg_qty_not_cancel = parsed_qty.notna() & (parsed_qty < 0) & (~is_cancel)
    v_pos_qty_on_cancel = parsed_qty.notna() & (parsed_qty > 0) & is_cancel

    # Exact duplicate detection across business columns
    dedup_subset = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]
    if deduplicate_exact_rows:
        # Canonicalize nulls/whitespace slightly for exact row comparison
        dup_frame = pd.DataFrame(
            {
                "InvoiceNo": inv_str,
                "StockCode": stock_str,
                "Description": desc_str,
                "Quantity": parsed_qty,
                "InvoiceDate": parsed_ts.astype(str),
                "UnitPrice": parsed_price,
                "CustomerID": cust_str.fillna(""),
                "Country": country_str,
            }
        )
        v_exact_dup = dup_frame.duplicated(keep="first")
    else:
        v_exact_dup = pd.Series(False, index=work.index)

    rule_masks: list[tuple[str, pd.Series]] = [
        ("DQ_MISSING_OR_INVALID_TYPE", v_missing_required | v_non_int_qty),
        ("DQ_NON_STANDARD_INVOICE", v_non_standard_invoice),
        ("DQ_ZERO_QUANTITY", v_zero_qty),
        ("DQ_NON_POSITIVE_UNIT_PRICE", v_non_positive_price),
        ("DQ_NEGATIVE_QTY_WITHOUT_CANCEL", v_neg_qty_not_cancel),
        ("DQ_CANCEL_WITH_POSITIVE_QTY", v_pos_qty_on_cancel),
        ("DQ_EXACT_DUPLICATE", v_exact_dup),
    ]

    any_violation = pd.Series(False, index=work.index)
    reason_lists: list[list[str]] = [[] for _ in range(len(work))]
    for rule_code, mask in rule_masks:
        any_violation = any_violation | mask
        for idx_pos in mask[mask].index:
            reason_lists[work.index.get_loc(idx_pos)].append(rule_code)

    primary_reasons = [reasons[0] if reasons else "" for reasons in reason_lists]
    all_reasons = ["|".join(reasons) for reasons in reason_lists]

    work["batch_id"] = batch_id
    work["rejection_reason"] = primary_reasons
    work["rejection_rules_all"] = all_reasons

    rejected_df = work.loc[any_violation].copy()
    valid_df = work.loc[~any_violation].drop(columns=["rejection_reason", "rejection_rules_all"]).copy()

    # Soft quality indicators (retained in Silver for analytical transparency)
    valid_inv = valid_df["InvoiceNo"].astype(str).str.strip()
    valid_cancellations = int(valid_inv.str.upper().str.startswith("C").sum())
    valid_missing_customer = int(valid_df["CustomerID"].isna().sum())
    valid_missing_desc = int(
        (valid_df["Description"].isna() | (valid_df["Description"].astype(str).str.strip() == "")).sum()
    )
    valid_unspecified_country = int(
        (valid_df["Country"].astype(str).str.strip() == "Unspecified").sum()
    )

    rule_breakdown = {code: int(mask.sum()) for code, mask in rule_masks if int(mask.sum()) > 0}
    primary_breakdown = (
        rejected_df["rejection_reason"].value_counts().to_dict() if not rejected_df.empty else {}
    )

    total_rows = int(len(work))
    valid_rows = int(len(valid_df))
    rejected_rows = int(len(rejected_df))
    pass_rate = round((valid_rows / total_rows * 100.0) if total_rows else 0.0, 4)

    metrics: dict[str, Any] = {
        "batch_id": batch_id,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "pass_rate_pct": pass_rate,
        "rule_violation_counts": rule_breakdown,
        "primary_rejection_counts": {str(k): int(v) for k, v in primary_breakdown.items()},
        "silver_profile": {
            "cancelled_transactions": valid_cancellations,
            "missing_customer_id": valid_missing_customer,
            "missing_description": valid_missing_desc,
            "unspecified_country": valid_unspecified_country,
        },
    }

    return ValidationOutcome(valid_df=valid_df, rejected_df=rejected_df, metrics=metrics)
