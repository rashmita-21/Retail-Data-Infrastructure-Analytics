"""Generate high-DPI data visuals, architecture diagrams, and standalone 12-slide PPTX presentation."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import load_config

# Color palette (Modern Executive Slate & Teal)
NAVY = RGBColor(15, 23, 42)        # #0F172A
SLATE = RGBColor(30, 41, 59)       # #1E293B
MUTED = RGBColor(100, 116, 139)    # #64748B
TEAL = RGBColor(14, 165, 233)      # #0EA5E9
EMERALD = RGBColor(16, 185, 129)   # #10B981
CORAL = RGBColor(239, 68, 68)      # #EF4444
AMBER = RGBColor(245, 158, 11)     # #F59E0B
LIGHT_BG = RGBColor(248, 250, 252) # #F8FAFC
WHITE = RGBColor(255, 255, 255)
BORDER = RGBColor(226, 232, 240)   # #E2E8F0

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial"]
plt.rcParams["axes.edgecolor"] = "#CBD5E1"
plt.rcParams["axes.linewidth"] = 0.8


def _draw_box(ax, x, y, w, h, title, subtitle="", facecolor="#1E293B", edgecolor="#0EA5E9", textcolor="#FFFFFF", subcolor="#CBD5E1", fontsize=9.5):
    box = patches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.015,rounding_size=0.02",
        facecolor=facecolor,
        edgecolor=edgecolor,
        linewidth=1.8,
    )
    ax.add_patch(box)
    if subtitle:
        ax.text(x + w / 2, y + h * 0.62, title, ha="center", va="center", fontsize=fontsize, fontweight="bold", color=textcolor)
        ax.text(x + w / 2, y + h * 0.30, subtitle, ha="center", va="center", fontsize=fontsize - 1.8, color=subcolor)
    else:
        ax.text(x + w / 2, y + h / 2, title, ha="center", va="center", fontsize=fontsize, fontweight="bold", color=textcolor)


def _draw_arrow(ax, x1, y1, x2, y2, color="#475569", label=""):
    ax.annotate(
        "",
        xy=(x2, y2),
        xytext=(x1, y1),
        arrowprops=dict(arrowstyle="-|>,head_width=0.25,head_length=0.4", lw=1.8, color=color),
    )
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mx, my + 0.025, label, ha="center", va="bottom", fontsize=7.5, color="#334155", fontweight="bold")


def generate_assets(cfg, assets_dir: Path) -> dict[str, Path]:
    assets_dir.mkdir(parents=True, exist_ok=True)
    reports = cfg.reports_dir
    db_path = cfg.warehouse_db_path
    sql_dir = cfg.sql_dir / "analytics"

    raw_prof = json.loads((reports / "raw_dataset_profile.json").read_text(encoding="utf-8"))
    dq_rep = json.loads((reports / "consolidated_data_quality_report.json").read_text(encoding="utf-8"))

    def q(filename: str) -> pd.DataFrame:
        with duckdb.connect(str(db_path), read_only=True) as conn:
            return conn.sql((sql_dir / filename).read_text(encoding="utf-8")).df()

    paths: dict[str, Path] = {}

    # 1. Chart: Raw Dataset Anomalies (Slide 3)
    anomalies = raw_prof["anomalies"]
    missing = raw_prof["missing_values"]
    dups = raw_prof["duplicates"]
    items = [
        ("Missing CustomerID", missing["CustomerID"], "#F59E0B"),
        ("Cancelled Invoices ('C')", anomalies["cancelled_invoices_prefix_C"], "#0EA5E9"),
        ("Exact Duplicate Rows", dups["exact_duplicate_excess_rows"], "#EF4444"),
        ("Non-Positive UnitPrice", anomalies["unit_price_non_positive"], "#EF4444"),
        ("Missing Description", missing["Description"], "#F59E0B"),
        ("Neg Qty w/o 'C' Prefix", anomalies["quantity_negative_without_C_prefix"], "#EF4444"),
        ("Unspecified Country", anomalies["unspecified_country_rows"], "#64748B"),
    ]
    items_rev = list(reversed(items))
    fig, ax = plt.subplots(figsize=(6.4, 4.2), dpi=240)
    labels = [i[0] for i in items_rev]
    vals = [i[1] for i in items_rev]
    colors = [i[2] for i in items_rev]
    bars = ax.barh(labels, vals, color=colors, height=0.6)
    ax.set_xscale("log")
    ax.set_xlim(100, 350000)
    for bar, v in zip(bars, vals):
        ax.text(v * 1.15, bar.get_y() + bar.get_height() / 2, f"{v:,}", va="center", fontsize=8.5, fontweight="bold", color="#0F172A")
    ax.set_title("Observed Raw Anomalies (Log Scale, N = 541,909)", fontsize=10.5, fontweight="bold", color="#0F172A", pad=10)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    p = assets_dir / "chart_raw_anomalies.png"
    fig.savefig(p, dpi=240, facecolor="white")
    plt.close(fig)
    paths["raw_anomalies"] = p

    # 2. Diagram: Local System Architecture (Slide 4)
    fig, ax = plt.subplots(figsize=(11.5, 4.3), dpi=240)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    _draw_box(ax, 0.02, 0.52, 0.14, 0.28, "UCI Dataset", "541,909 Rows\nExcel Source", "#0F172A", "#64748B")
    _draw_box(ax, 0.21, 0.52, 0.14, 0.28, "Raw Ingester", "Schema + SHA-256\n17 Sim Batches", "#1E293B", "#0EA5E9")
    _draw_box(ax, 0.40, 0.52, 0.15, 0.28, "DQ Engine", "Rules & Dedup\n98.57% Pass Rate", "#1E293B", "#0EA5E9")
    _draw_box(ax, 0.60, 0.52, 0.16, 0.28, "PySpark ETL", "Parquet Partitions\nSHA-256 Line PK", "#0369A1", "#38BDF8")
    _draw_box(ax, 0.81, 0.52, 0.17, 0.28, "DuckDB Warehouse", "Star Schema\n534,128 Fact Rows", "#065F46", "#10B981")

    _draw_box(ax, 0.40, 0.10, 0.15, 0.24, "Rejected Zone", "7,774 Quarantined\nParquet + DQ JSON", "#7F1D1D", "#EF4444")
    _draw_box(ax, 0.21, 0.10, 0.14, 0.24, "SQLite State", "Checkpoints &\nWatermarks", "#334155", "#94A3B8")
    _draw_box(ax, 0.81, 0.10, 0.17, 0.24, "16 SQL Queries", "RFM, Cohorts,\nPareto & Trends", "#1E293B", "#10B981")

    _draw_arrow(ax, 0.165, 0.66, 0.205, 0.66)
    _draw_arrow(ax, 0.355, 0.66, 0.395, 0.66)
    _draw_arrow(ax, 0.555, 0.66, 0.595, 0.66)
    _draw_arrow(ax, 0.765, 0.66, 0.805, 0.66)
    ax.text(0.375, 0.84, "Bronze", ha="center", fontsize=8, fontweight="bold", color="#0284C7")
    ax.text(0.575, 0.84, "Silver", ha="center", fontsize=8, fontweight="bold", color="#0284C7")
    ax.text(0.785, 0.84, "Gold", ha="center", fontsize=8, fontweight="bold", color="#059669")
    _draw_arrow(ax, 0.475, 0.51, 0.475, 0.35, color="#EF4444")
    _draw_arrow(ax, 0.28, 0.51, 0.28, 0.35, color="#64748B")
    _draw_arrow(ax, 0.895, 0.51, 0.895, 0.35, color="#10B981")
    ax.text(0.50, 0.92, "Local Medallion Lakehouse -> Dimensional Star Schema Pipeline", ha="center", fontsize=12, fontweight="bold", color="#0F172A")
    plt.tight_layout()
    p = assets_dir / "diagram_local_arch.png"
    fig.savefig(p, dpi=240, facecolor="#F8FAFC")
    plt.close(fig)
    paths["local_arch"] = p

    # 3. Chart: DQ Funnel / Quarantine Breakdown (Slide 5)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(6.5, 4.1), dpi=240, gridspec_kw={"width_ratios": [1.1, 1.0]})
    stages = ["Raw\nInput", "Silver\nValid", "Warehouse\nFact Table"]
    counts = [dq_rep["total_rows_processed"], dq_rep["total_valid_rows"], 534128]
    bars1 = ax1.bar(stages, [c / 1e3 for c in counts], color=["#334155", "#0EA5E9", "#10B981"], width=0.55)
    ax1.set_ylim(450, 560)
    for b, c in zip(bars1, counts):
        ax1.text(b.get_x() + b.get_width() / 2, b.get_height() + 3, f"{c:,}", ha="center", fontsize=8, fontweight="bold", color="#0F172A")
    ax1.set_title("Record Conservation (K)", fontsize=9.5, fontweight="bold", color="#0F172A")
    ax1.grid(axis="y", linestyle="--", alpha=0.4)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    rej_items = list(dq_rep["primary_rejection_totals"].items())
    r_labels = [k.replace("DQ_", "").replace("_", "\n") for k, _ in rej_items]
    r_vals = [v for _, v in rej_items]
    bars2 = ax2.bar(r_labels, r_vals, color=["#EF4444", "#F59E0B", "#64748B"], width=0.55)
    for b, v in zip(bars2, r_vals):
        ax2.text(b.get_x() + b.get_width() / 2, b.get_height() + 120, f"{v:,}", ha="center", fontsize=8, fontweight="bold", color="#0F172A")
    ax2.set_ylim(0, 6200)
    ax2.set_title("Quarantined Rows (7,774)", fontsize=9.5, fontweight="bold", color="#0F172A")
    ax2.tick_params(axis="x", labelsize=7)
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    plt.tight_layout()
    p = assets_dir / "chart_dq_funnel.png"
    fig.savefig(p, dpi=240, facecolor="white")
    plt.close(fig)
    paths["dq_funnel"] = p

    # 4. Diagram: Incremental & Late-Arriving Mechanics (Slide 6)
    fig, ax = plt.subplots(figsize=(11.2, 2.7), dpi=240)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    _draw_box(ax, 0.02, 0.28, 0.21, 0.48, "1. Backfill Batch", "batch_0001\n2010-12-01..2011-11-22", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_box(ax, 0.27, 0.28, 0.21, 0.48, "2. 15 Daily Batches", "batch_0002..0016\nWatermark -> 2011-12-09", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_box(ax, 0.52, 0.28, 0.22, 0.48, "3. Late-Arriving Batch", "batch_9999 (250 rows)\nBackfills 2011-06..11", "#7C2D12", "#F59E0B", fontsize=9)
    _draw_box(ax, 0.78, 0.28, 0.20, 0.48, "4. Idempotent Rerun", "17/17 Skipped (SHA-256)\n0 Duplicate Fact Rows", "#065F46", "#10B981", fontsize=9)
    _draw_arrow(ax, 0.23, 0.52, 0.27, 0.52)
    _draw_arrow(ax, 0.48, 0.52, 0.52, 0.52)
    _draw_arrow(ax, 0.74, 0.52, 0.78, 0.52)
    plt.tight_layout()
    p = assets_dir / "diagram_incremental_late.png"
    fig.savefig(p, dpi=240, facecolor="#F8FAFC")
    plt.close(fig)
    paths["incremental_late"] = p

    # 5. Diagram: Star Schema ERD (Slide 7)
    fig, ax = plt.subplots(figsize=(6.4, 4.4), dpi=240)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    _draw_box(ax, 0.31, 0.34, 0.38, 0.32, "analytics.fact_sales", "534,128 rows | Grain: Invoice Line\ntransaction_line_id (PK)\ngross / cancelled / net_amount", "#0F172A", "#10B981", fontsize=9.5)
    _draw_box(ax, 0.02, 0.70, 0.27, 0.24, "dim_date", "305 rows\ndate_key (PK)", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_box(ax, 0.71, 0.70, 0.27, 0.24, "dim_customer", "4,372 rows\n(-1 = GUEST)", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_box(ax, 0.02, 0.06, 0.27, 0.24, "dim_product", "3,827 rows\n9 fee SKUs flagged", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_box(ax, 0.71, 0.06, 0.27, 0.24, "dim_country", "38 rows\nregion_group", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_arrow(ax, 0.29, 0.76, 0.36, 0.66, color="#0EA5E9")
    _draw_arrow(ax, 0.71, 0.76, 0.64, 0.66, color="#0EA5E9")
    _draw_arrow(ax, 0.29, 0.24, 0.36, 0.34, color="#0EA5E9")
    _draw_arrow(ax, 0.71, 0.24, 0.64, 0.34, color="#0EA5E9")
    ax.text(0.50, 0.96, "DuckDB Dimensional Star Schema", ha="center", fontsize=11, fontweight="bold", color="#0F172A")
    plt.tight_layout()
    p = assets_dir / "diagram_star_schema.png"
    fig.savefig(p, dpi=240, facecolor="#F8FAFC")
    plt.close(fig)
    paths["star_schema"] = p

    # 6. Chart: Monthly Revenue & Cancellation Impact (Slide 8)
    m_df = q("01_monthly_revenue_trend.sql")
    c_df = q("07_cancellation_impact_on_revenue.sql")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11.2, 4.4), dpi=240, sharex=True, gridspec_kw={"height_ratios": [1.6, 1.0]})
    x = np.arange(len(m_df))
    ax1.plot(x, m_df["gross_revenue"] / 1e6, marker="o", linewidth=2.2, color="#0EA5E9", label="Gross Revenue ($M)")
    ax1.plot(x, m_df["net_revenue"] / 1e6, marker="s", linewidth=2.2, color="#10B981", label="Net Revenue ($M)")
    ax1.fill_between(x, m_df["net_revenue"] / 1e6, m_df["gross_revenue"] / 1e6, color="#EF4444", alpha=0.18, label="Cancelled Gap")
    ax1.set_title("Monthly Gross vs. Net Revenue & Cancellation Impact (Dec 2010 - Dec 2011)", fontsize=10.5, fontweight="bold", color="#0F172A")
    ax1.set_ylabel("Revenue ($M)", fontsize=9)
    ax1.grid(axis="y", linestyle="--", alpha=0.4)
    ax1.legend(loc="upper left", fontsize=8.5)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    ax2.bar(x, c_df["cancellation_impact_pct"], color="#EF4444", alpha=0.75, width=0.55)
    ax2.axhline(float(c_df["overall_avg_impact_pct"].iloc[0]), color="#0F172A", linestyle="--", linewidth=1.3, label=f"Avg: {c_df['overall_avg_impact_pct'].iloc[0]}%")
    ax2.set_ylim(0, max(38, float(c_df["cancellation_impact_pct"].max()) + 6))
    ax2.set_ylabel("Cancel %", fontsize=9)
    ax2.set_xticks(x)
    ax2.set_xticklabels(m_df["year_month"], rotation=30, fontsize=8.5)
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.legend(loc="upper left", fontsize=8.5)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    plt.tight_layout()
    p = assets_dir / "chart_monthly_revenue.png"
    fig.savefig(p, dpi=240, facecolor="white")
    plt.close(fig)
    paths["monthly_revenue"] = p

    # 7. Chart: Top 10 Products & Pareto 80/20 (Slide 9)
    prod_df = q("02_top_products_by_revenue.sql").head(10).iloc[::-1]
    pareto_df = q("15_pareto_80_20_product_revenue.sql")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.3), dpi=240, gridspec_kw={"width_ratios": [1.45, 1.0]})
    b1 = ax1.barh(prod_df["product_description"].str.slice(0, 30), prod_df["net_revenue"] / 1e3, color="#0EA5E9", height=0.62)
    for bar, v in zip(b1, prod_df["net_revenue"] / 1e3):
        ax1.text(v + 2, bar.get_y() + bar.get_height() / 2, f"${v:,.1f}K", va="center", fontsize=7.5, fontweight="bold", color="#0F172A")
    ax1.set_xlim(0, 195)
    ax1.set_title("Top 10 Products by Net Revenue ($K)", fontsize=10, fontweight="bold", color="#0F172A")
    ax1.grid(axis="x", linestyle="--", alpha=0.4)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    px = np.arange(len(pareto_df))
    w = 0.36
    ax2.bar(px - w / 2, pareto_df["pct_of_skus"], width=w, color="#64748B", label="% of SKUs")
    ax2.bar(px + w / 2, pareto_df["pct_of_revenue"], width=w, color="#10B981", label="% of Revenue")
    for i, (s, r) in enumerate(zip(pareto_df["pct_of_skus"], pareto_df["pct_of_revenue"])):
        ax2.text(i - w / 2, s + 1.5, f"{s:.1f}%", ha="center", fontsize=7.5, fontweight="bold")
        ax2.text(i + w / 2, r + 1.5, f"{r:.1f}%", ha="center", fontsize=7.5, fontweight="bold")
    ax2.set_xticks(px)
    ax2.set_xticklabels(["Top 80%\nDrivers", "Next 15%\nLong-Tail", "Bottom 5%\nNiche"], fontsize=8.5)
    ax2.set_ylim(0, 95)
    ax2.set_title("Pareto 80/20 Concentration", fontsize=10, fontweight="bold", color="#0F172A")
    ax2.legend(fontsize=8)
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    plt.tight_layout()
    p = assets_dir / "chart_top_products_pareto.png"
    fig.savefig(p, dpi=240, facecolor="white")
    plt.close(fig)
    paths["top_products_pareto"] = p

    # 8. Chart: RFM Segments & Cohort Retention Heatmap (Slide 10)
    rfm_df = q("04_customer_rfm_segmentation.sql")
    coh_df = q("08_cohort_retention_analysis.sql")
    piv = coh_df.pivot(index="cohort_month", columns="months_since_first_purchase", values="retention_rate_pct").fillna(0)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.3), dpi=240, gridspec_kw={"width_ratios": [1.0, 1.25]})
    bars = ax1.barh(rfm_df["rfm_segment"].iloc[::-1], rfm_df["total_segment_revenue"].iloc[::-1] / 1e6, color="#10B981", height=0.58)
    for bar, v in zip(bars, rfm_df["total_segment_revenue"].iloc[::-1] / 1e6):
        ax1.text(v + 0.08, bar.get_y() + bar.get_height() / 2, f"${v:.2f}M", va="center", fontsize=8, fontweight="bold", color="#0F172A")
    ax1.set_xlim(0, 5.1)
    ax1.set_title("Customer RFM Segment Revenue ($M)", fontsize=10, fontweight="bold", color="#0F172A")
    ax1.grid(axis="x", linestyle="--", alpha=0.4)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    im = ax2.imshow(piv.values, cmap="YlGnBu", aspect="auto", vmin=0, vmax=100)
    ax2.set_xticks(np.arange(len(piv.columns)))
    ax2.set_xticklabels([f"M+{c}" for c in piv.columns], fontsize=8)
    ax2.set_yticks(np.arange(len(piv.index)))
    ax2.set_yticklabels(piv.index, fontsize=7.5)
    ax2.set_title("Monthly Cohort Retention (%)", fontsize=10, fontweight="bold", color="#0F172A")
    for r in range(piv.shape[0]):
        for c in range(piv.shape[1]):
            val = piv.values[r, c]
            if val > 0:
                ax2.text(c, r, f"{val:.0f}", ha="center", va="center", fontsize=6.5, color="white" if val > 55 else "#0F172A")
    plt.tight_layout()
    p = assets_dir / "chart_rfm_cohort.png"
    fig.savefig(p, dpi=240, facecolor="white")
    plt.close(fig)
    paths["rfm_cohort"] = p

    # 9. Diagram: AWS Cloud-Ready Architecture (Slide 11)
    fig, ax = plt.subplots(figsize=(11.2, 3.0), dpi=240)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    _draw_box(ax, 0.02, 0.30, 0.16, 0.46, "Amazon S3", "Raw Bucket\n(AES256 + Glacier)", "#1E293B", "#F59E0B", fontsize=9)
    _draw_box(ax, 0.22, 0.30, 0.16, 0.46, "AWS Lambda", "S3 ObjectCreated\nIngestion Trigger", "#1E293B", "#F59E0B", fontsize=9)
    _draw_box(ax, 0.42, 0.30, 0.19, 0.46, "AWS Glue 4.0", "PySpark DQ & ETL\n+ Glue Data Catalog", "#0369A1", "#38BDF8", fontsize=9)
    _draw_box(ax, 0.65, 0.30, 0.15, 0.46, "S3 Curated", "Partitioned Parquet\n(Gold Layer)", "#1E293B", "#0EA5E9", fontsize=9)
    _draw_box(ax, 0.84, 0.30, 0.14, 0.46, "Amazon Redshift", "Star Schema\nCOPY + MERGE", "#065F46", "#10B981", fontsize=9)
    _draw_arrow(ax, 0.18, 0.53, 0.22, 0.53)
    _draw_arrow(ax, 0.38, 0.53, 0.42, 0.53)
    _draw_arrow(ax, 0.61, 0.53, 0.65, 0.53)
    _draw_arrow(ax, 0.80, 0.53, 0.84, 0.53)
    ax.text(0.50, 0.90, "CloudFormation IaC + Least-Privilege IAM Target Architecture (Prepared, Not Deployed)", ha="center", fontsize=10.5, fontweight="bold", color="#0F172A")
    plt.tight_layout()
    p = assets_dir / "diagram_aws_arch.png"
    fig.savefig(p, dpi=240, facecolor="#F8FAFC")
    plt.close(fig)
    paths["aws_arch"] = p

    return paths


def _set_bg(slide, color: RGBColor) -> None:
    bg = slide.background
    fill = bg.fill
    fill.solid()
    fill.fore_color.rgb = color


def _add_header(slide, title: str, subtitle: str, page_num: int) -> None:
    # Top accent bar
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0), Inches(13.333), Inches(0.10))
    bar.fill.solid()
    bar.fill.fore_color.rgb = TEAL
    bar.line.fill.background()

    tb = slide.shapes.add_textbox(Inches(0.6), Inches(0.25), Inches(11.5), Inches(0.85))
    tf = tb.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = title
    p.font.size = Pt(24)
    p.font.bold = True
    p.font.color.rgb = NAVY

    p2 = tf.add_paragraph()
    p2.text = subtitle
    p2.font.size = Pt(12.5)
    p2.font.color.rgb = MUTED

    # Footer
    ft = slide.shapes.add_textbox(Inches(0.6), Inches(7.12), Inches(12.1), Inches(0.3))
    fp = ft.text_frame.paragraphs[0]
    fp.text = f"Retail Data Infrastructure & Analytics Platform  |  Slide {page_num} of 12"
    fp.font.size = Pt(9.5)
    fp.font.color.rgb = MUTED
    fp.alignment = PP_ALIGN.RIGHT


def _add_card(slide, left: float, top: float, width: float, height: float, title: str, bullets: list[str], accent: RGBColor = TEAL) -> None:
    card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(left), Inches(top), Inches(width), Inches(height))
    card.fill.solid()
    card.fill.fore_color.rgb = WHITE
    card.line.color.rgb = BORDER
    card.line.width = Pt(1)
    card.adjustments[0] = 0.03

    stripe = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(left), Inches(top), Inches(width), Inches(0.07))
    stripe.fill.solid()
    stripe.fill.fore_color.rgb = accent
    stripe.line.fill.background()

    tb = slide.shapes.add_textbox(Inches(left + 0.22), Inches(top + 0.18), Inches(width - 0.44), Inches(height - 0.30))
    tf = tb.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = title
    p.font.size = Pt(14)
    p.font.bold = True
    p.font.color.rgb = NAVY
    p.space_after = Pt(8)

    for b in bullets:
        bp = tf.add_paragraph()
        bp.text = f"•  {b}"
        bp.font.size = Pt(11.5)
        bp.font.color.rgb = SLATE
        bp.space_after = Pt(5)


def _add_notes(slide, notes_text: str) -> None:
    slide.notes_slide.notes_text_frame.text = notes_text


def build_pptx(cfg) -> Path:
    pres_dir = cfg.project_root / "presentation"
    assets = generate_assets(cfg, pres_dir / "assets")

    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank = prs.slide_layouts[6]

    # SLIDE 1: Title Slide
    s1 = prs.slides.add_slide(blank)
    _set_bg(s1, NAVY)
    accent_bar = s1.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), Inches(1.3), Inches(1.2), Inches(0.09))
    accent_bar.fill.solid()
    accent_bar.fill.fore_color.rgb = TEAL
    accent_bar.line.fill.background()

    tb1 = s1.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(11.5), Inches(2.5))
    tf1 = tb1.text_frame
    tf1.word_wrap = True
    p = tf1.paragraphs[0]
    p.text = "Retail Data Infrastructure &\nAnalytics Platform"
    p.font.size = Pt(42)
    p.font.bold = True
    p.font.color.rgb = WHITE
    p2 = tf1.add_paragraph()
    p2.text = "Production-Inspired Lakehouse, PySpark ETL, DuckDB Star Schema & AWS-Ready Architecture"
    p2.font.size = Pt(18)
    p2.font.color.rgb = RGBColor(148, 163, 184)
    p2.space_before = Pt(14)

    kpis = [
        ("541,909", "Raw UCI Transactions"),
        ("98.57%", "Data Quality Pass Rate"),
        ("534,128", "Fact Rows Loaded"),
        ("16 SQL + 11 Tests", "Analytics & Pytest Verified"),
    ]
    for idx, (val, lbl) in enumerate(kpis):
        left = 0.8 + idx * 3.0
        box = s1.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(left), Inches(4.7), Inches(2.75), Inches(1.3))
        box.fill.solid()
        box.fill.fore_color.rgb = SLATE
        box.line.color.rgb = TEAL
        box.line.width = Pt(1.2)
        ktf = box.text_frame
        ktf.word_wrap = True
        ktf.vertical_anchor = MSO_ANCHOR.MIDDLE
        kp = ktf.paragraphs[0]
        kp.text = val
        kp.font.size = Pt(22)
        kp.font.bold = True
        kp.font.color.rgb = EMERALD if idx == 2 else TEAL
        kp.alignment = PP_ALIGN.CENTER
        kp2 = ktf.add_paragraph()
        kp2.text = lbl
        kp2.font.size = Pt(11)
        kp2.font.color.rgb = RGBColor(203, 213, 225)
        kp2.alignment = PP_ALIGN.CENTER
    _add_notes(s1, "Title slide introducing the end-to-end Retail Data Infrastructure & Analytics Platform built on the official UCI Online Retail dataset.")

    # SLIDE 2: Business Problem & Engineering Objectives
    s2 = prs.slides.add_slide(blank)
    _set_bg(s2, LIGHT_BG)
    _add_header(s2, "Business Problem & Engineering Objectives", "Why raw retail POS exports break naive BI dashboards", 2)
    _add_card(
        s2, 0.6, 1.35, 5.9, 5.4,
        "Data Challenges in Raw Retail Logs",
        [
            "24.93% of rows (135,080) lack CustomerID — naive inner joins silently drop ~$1.74M in guest sales.",
            "5,268 exact duplicate rows inflate revenue unless caught at ingestion and warehouse upsert.",
            "9,288 cancelled invoices ('C' prefix) and 2,517 zero/negative price entries distort gross vs. net reporting.",
            "1,336 negative-quantity write-offs and 3 bad-debt adjustments lack cancellation prefixes.",
            "Out-of-order late-arriving records break naive append-only batch pipelines.",
        ],
        accent=CORAL,
    )
    _add_card(
        s2, 6.8, 1.35, 5.9, 5.4,
        "Platform Engineering Objectives",
        [
            "Enforce automated Data Quality gates routing bad rows to a Quarantine Parquet zone with rule audit codes.",
            "Preserve cancelled invoices and guest checkouts via explicit flags and surrogate key CustomerKey = -1.",
            "Process records via PySpark into Hive-partitioned Parquet (invoice_year_month / batch_id).",
            "Guarantee idempotent reruns and safe late-arriving backfills via SQLite watermarks and SHA-256 PKs.",
            "Serve 16 analytical SQL queries on a DuckDB Star Schema and prepare AWS CloudFormation/Glue/Redshift IaC.",
        ],
        accent=EMERALD,
    )
    _add_notes(s2, "Contrast the raw data quality issues found in UCI Online Retail with the platform's engineering goals.")

    # SLIDE 3: Dataset Profile & Historical Simulation
    s3 = prs.slides.add_slide(blank)
    _set_bg(s3, LIGHT_BG)
    _add_header(s3, "Dataset Profile & Historical Batch Simulation", "Official UCI Online Retail Dataset (ID 352): Dec 2010 - Dec 2011", 3)
    _add_card(
        s3, 0.6, 1.35, 5.3, 5.4,
        "Source & Simulation Design",
        [
            "541,909 total rows | 25,900 invoices | 4,070 SKUs | 4,372 CustomerIDs | 38 countries.",
            "Historical-data simulation slices InvoiceDate chronologically with 100% row conservation:",
            "Batch 0001: Historical backfill (491,984 rows).",
            "Batches 0002-0016: 15 daily incremental simulation CSVs.",
            "Batch 9999: 250 real historical records held back to test out-of-order late arrival.",
        ],
        accent=TEAL,
    )
    s3.shapes.add_picture(str(assets["raw_anomalies"]), Inches(6.15), Inches(1.4), width=Inches(6.6))
    _add_notes(s3, "Walk through the exact verified statistics profiled from Online Retail.xlsx and how 17 reproducible simulation batches were built.")

    # SLIDE 4: Local System Architecture
    s4 = prs.slides.add_slide(blank)
    _set_bg(s4, LIGHT_BG)
    _add_header(s4, "End-to-End Local System Architecture", "Medallion Lakehouse + Checkpoint Store + Dimensional Warehouse", 4)
    s4.shapes.add_picture(str(assets["local_arch"]), Inches(0.65), Inches(1.3), width=Inches(12.0))
    _add_card(
        s4, 0.65, 5.85, 12.0, 1.15,
        "Core Local Technology Stack",
        [
            "Python 3.12  |  PySpark 4.3.0 (OpenJDK 17)  |  Apache Parquet (Snappy)  |  DuckDB 2.0.0  |  SQLite Checkpoints  |  Pydantic v2  |  Tenacity Retries  |  pytest",
        ],
        accent=NAVY,
    )
    _add_notes(s4, "Explain data flow from raw ingestion through Bronze, Silver/Rejected, Gold partitioned Parquet, and DuckDB Star Schema.")

    # SLIDE 5: Data Quality & Medallion Quarantine
    s5 = prs.slides.add_slide(blank)
    _set_bg(s5, LIGHT_BG)
    _add_header(s5, "Automated Data Quality & Quarantine Layer", "98.57% pass rate across 541,909 rows with full auditability", 5)
    s5.shapes.add_picture(str(assets["dq_funnel"]), Inches(0.6), Inches(1.4), width=Inches(6.6))
    _add_card(
        s5, 7.4, 1.35, 5.3, 5.4,
        "Validation & Handling Decisions",
        [
            "Hard Rejections (7,774 rows -> Rejected Parquet): 5,256 exact duplicates, 2,515 zero/negative unit prices, 3 non-standard bad-debt invoices.",
            "Every rejected row retains both primary rejection_reason and pipe-delimited rejection_rules_all.",
            "Cancellations (9,251 valid rows) retained with is_cancelled = true for accurate net revenue.",
            "Guest checkouts (132,564 valid rows) retained and mapped to dim_customer key = -1.",
            "7 cross-batch exact duplicates caught by SHA-256 primary key during warehouse upsert -> 534,128 unique rows.",
        ],
        accent=CORAL,
    )
    _add_notes(s5, "Highlight why separating hard rejections from valid business edge cases (cancellations, guests) prevents revenue distortion.")

    # SLIDE 6: Incremental, Idempotency & Late-Arriving Data
    s6 = prs.slides.add_slide(blank)
    _set_bg(s6, LIGHT_BG)
    _add_header(s6, "Incremental Processing, Idempotency & Late Arrivals", "Safe retries, monotonic watermarks, and deterministic primary keys", 6)
    s6.shapes.add_picture(str(assets["incremental_late"]), Inches(0.7), Inches(1.3), width=Inches(11.9))
    _add_card(
        s6, 0.7, 4.35, 3.75, 2.55,
        "SHA-256 Batch Fingerprinting",
        [
            "CheckpointStore records batch_id, file_sha256, and stage status.",
            "Re-running loaded batches skips 17/17 in ~0.004s per batch.",
        ],
        accent=TEAL,
    )
    _add_card(
        s6, 4.78, 4.35, 3.75, 2.55,
        "Dynamic Parquet & Upsert",
        [
            "Partitioned by (invoice_year_month, batch_id) with dynamic overwrite.",
            "DuckDB INSERT ... ON CONFLICT (transaction_line_id) DO UPDATE prevents duplicates even on --force.",
        ],
        accent=EMERALD,
    )
    _add_card(
        s6, 8.85, 4.35, 3.75, 2.55,
        "Monotonic Watermark Safety",
        [
            "high_watermark_ts remains at 2011-12-09 12:50:00 when batch_9999 arrives.",
            "dim_customer updates first_purchase_ts = LEAST(...) accurately.",
        ],
        accent=AMBER,
    )
    _add_notes(s6, "Explain how idempotency works at three layers: ingestion checksums, Spark dynamic partitions, and DuckDB primary-key upserts.")

    # SLIDE 7: Dimensional Star Schema
    s7 = prs.slides.add_slide(blank)
    _set_bg(s7, LIGHT_BG)
    _add_header(s7, "Dimensional Data Warehouse (DuckDB Star Schema)", "Invoice-line grain supporting full financial reconciliation", 7)
    s7.shapes.add_picture(str(assets["star_schema"]), Inches(0.6), Inches(1.35), width=Inches(6.5))
    _add_card(
        s7, 7.35, 1.35, 5.35, 5.4,
        "Verified Warehouse Totals & Design",
        [
            "fact_sales: 534,128 rows | Gross: $10,631,048.74 | Cancelled: $893,979.73 | Net: $9,737,069.01.",
            "dim_customer: 4,372 rows (4,371 identified + CustomerKey = -1 for GUEST).",
            "dim_product: 3,827 rows (3,818 SKUs + 9 non-product codes like POST, D, BANK CHARGES).",
            "dim_date: 305 active calendar days | dim_country: 38 countries (incl. Unspecified).",
            "etl_batch_audit: 17 batch lineage rows verifying row counts and late-arrival flags.",
        ],
        accent=EMERALD,
    )
    _add_notes(s7, "Show exact penny-to-penny reconciliation ($10,631,048.74 - $893,979.73 = $9,737,069.01) across the Star Schema.")

    # SLIDE 8: Revenue & Cancellation Analytics
    s8 = prs.slides.add_slide(blank)
    _set_bg(s8, LIGHT_BG)
    _add_header(s8, "Revenue Performance & Cancellation Impact", "13-month seasonality and gross-to-net erosion analysis (SQL 01 & 07)", 8)
    s8.shapes.add_picture(str(assets["monthly_revenue"]), Inches(0.8), Inches(1.3), width=Inches(11.7))
    _add_card(
        s8, 0.8, 5.85, 11.7, 1.15,
        "Key Financial Findings",
        [
            "Average monthly cancellation impact is 9.25% of gross revenue, with a sharp spike in Jan 2011 (19.04% / $131.4K cancelled) and strong Q4 peak in Nov 2011.",
        ],
        accent=TEAL,
    )
    _add_notes(s8, "Discuss monthly revenue growth, Q4 seasonality, and the Jan 2011 cancellation spike detected by SQL queries 01 and 07.")

    # SLIDE 9: Product Pareto & Geographic Concentration
    s9 = prs.slides.add_slide(blank)
    _set_bg(s9, LIGHT_BG)
    _add_header(s9, "Product Drivers, Pareto 80/20 & Country Mix", "SKU revenue concentration and international footprint (SQL 02, 03 & 15)", 9)
    s9.shapes.add_picture(str(assets["top_products_pareto"]), Inches(0.8), Inches(1.3), width=Inches(11.7))
    _add_card(
        s9, 0.8, 5.85, 11.7, 1.15,
        "Product & Country Takeaways",
        [
            "21.90% of SKUs (831 items) drive 79.98% ($7.82M) of product revenue  |  Top SKU: REGENCY CAKESTAND 3 TIER ($164.5K)  |  UK = 83.99% ($8.18M), NL = 2.92%, EIRE = 2.70%.",
        ],
        accent=EMERALD,
    )
    _add_notes(s9, "Show how SQL query 15 confirms a classic 80/20 Pareto curve and query 03 quantifies UK vs. European wholesale revenue.")

    # SLIDE 10: Customer RFM & Cohort Retention
    s10 = prs.slides.add_slide(blank)
    _set_bg(s10, LIGHT_BG)
    _add_header(s10, "Customer Analytics: RFM Segmentation & Cohort Retention", "Behavioral value tiers and longitudinal retention (SQL 04, 05, 08 & 13)", 10)
    s10.shapes.add_picture(str(assets["rfm_cohort"]), Inches(0.8), Inches(1.3), width=Inches(11.7))
    _add_card(
        s10, 0.8, 5.85, 11.7, 1.15,
        "Customer Insights",
        [
            "Champions RFM tier drives ~$4.2M+ in net spend (avg ~16 orders, ~6d recency)  |  Top CLV Customer #14646 (Netherlands): $279.5K across 73 orders  |  Dec-2010 cohort retains 36.6% at M+1.",
        ],
        accent=TEAL,
    )
    _add_notes(s10, "Review RFM segmentation results and the monthly acquisition cohort retention matrix computed in DuckDB SQL.")

    # SLIDE 11: AWS Cloud-Ready Architecture
    s11 = prs.slides.add_slide(blank)
    _set_bg(s11, LIGHT_BG)
    _add_header(s11, "AWS Cloud-Ready Target Architecture", "Infrastructure-as-Code prepared and separated from verified local runtime", 11)
    s11.shapes.add_picture(str(assets["aws_arch"]), Inches(0.8), Inches(1.25), width=Inches(11.7))
    _add_card(
        s11, 0.8, 4.45, 5.7, 2.45,
        "Prepared AWS Artifacts (Not Deployed)",
        [
            "CloudFormation stack (S3 4-zone buckets, Glacier lifecycle, Glue Crawler).",
            "2 AWS Glue 4.0 PySpark jobs + S3-triggered Lambda ingestion handler.",
            "3 least-privilege IAM policies + Redshift DISTKEY/SORTKEY DDL & COPY SQL.",
        ],
        accent=AMBER,
    )
    _add_card(
        s11, 6.8, 4.45, 5.7, 2.45,
        "Cost & Deployment Governance",
        [
            "Zero cloud spend incurred during local verification.",
            "Estimated Glue 4.0 cost: ~$0.03 per 2-minute test batch (2x G.1X DPUs).",
            "Redshift Serverless auto-pause + S3 expiration rules prevent idle charges.",
        ],
        accent=NAVY,
    )
    _add_notes(s11, "Clearly distinguish the verified local implementation from the prepared-but-undeployed AWS CloudFormation, Glue, Lambda, and Redshift assets.")

    # SLIDE 12: Testing, Limitations & Roadmap
    s12 = prs.slides.add_slide(blank)
    _set_bg(s12, LIGHT_BG)
    _add_header(s12, "Automated Testing, Limitations & Roadmap", "End-to-end quality assurance and production evolution path", 12)
    _add_card(
        s12, 0.6, 1.35, 5.9, 5.4,
        "Automated Pytest Verification (11/11 Passed)",
        [
            "test_ingestion.py (4): Schema checks, empty-file rejection, SHA-256 skip.",
            "test_validation.py (2): Rule splits, quarantine Parquet & DQ reports.",
            "test_transformation_spark.py (1): Derived metrics & partitioned Parquet.",
            "test_incremental_and_idempotency.py (1): Zero duplicates on forced reruns.",
            "test_late_arriving_data.py (1): Non-regressing watermark & history backfill.",
            "test_warehouse.py & test_analytics_sql.py (2): Star schema FKs & all 16 SQL queries.",
        ],
        accent=EMERALD,
    )
    _add_card(
        s12, 6.8, 1.35, 5.9, 5.4,
        "Known Limitations & Future Improvements",
        [
            "Historical simulation slices static Excel dates rather than a live Kafka/Kinesis stream.",
            "Windows local PySpark requires winutils.exe/hadoop.dll (bundled in .tools/hadoop).",
            "PySpark 4.3 emits a non-blocking FutureWarning with Pandas 3.0.0.",
            "Roadmap: SCD Type 2 tracking on dim_product & dim_customer.",
            "Roadmap: Apache Iceberg / Delta Lake ACID tables and GitHub Actions CI.",
        ],
        accent=TEAL,
    )
    _add_notes(s12, "Summarize test coverage, honest limitations, and concrete next engineering steps.")

    out_pptx = pres_dir / "Retail_Data_Platform_Presentation.pptx"
    prs.save(str(out_pptx))
    return out_pptx


def main() -> None:
    cfg = load_config()
    out_path = build_pptx(cfg)
    print(f"Presentation generated at: {out_path}")


if __name__ == "__main__":
    main()
