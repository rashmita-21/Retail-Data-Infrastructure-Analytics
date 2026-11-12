"""PySpark Silver-to-Gold ETL transformations, deduplication, derived metrics, and Parquet writes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.logging_utils import get_logger
from src.storage.checkpoint_store import CheckpointStore
from src.storage.lake_manager import LakeManager

logger = get_logger("transformation.etl_job")


@dataclass
class SparkETLResult:
    batch_id: str
    curated_row_count: int
    curated_transactions_path: Path
    monthly_summary_rows: int
    product_summary_rows: int
    customer_summary_rows: int


class SparkRetailETL:
    """Executes PySpark transformations from Silver Parquet into Curated Gold Parquet datasets."""

    def __init__(
        self,
        spark: SparkSession,
        lake_manager: LakeManager,
        checkpoint_store: CheckpointStore,
        special_non_product_codes: Iterable[str],
    ) -> None:
        self.spark = spark
        self.lake_manager = lake_manager
        self.checkpoint_store = checkpoint_store
        self.special_codes = [c.upper().strip() for c in special_non_product_codes]

    def transform_silver_df(self, df: DataFrame, batch_id: str) -> DataFrame:
        """Apply cleaning, type conversions, deduplication, and derived metrics in PySpark."""
        cleaned = (
            df.withColumn("invoice_no", F.trim(F.col("InvoiceNo").cast(T.StringType())))
            .withColumn("stock_code", F.upper(F.trim(F.col("StockCode").cast(T.StringType()))))
            .withColumn(
                "_raw_desc",
                F.trim(F.col("Description").cast(T.StringType())),
            )
            .withColumn(
                "is_missing_description",
                F.col("_raw_desc").isNull() | (F.col("_raw_desc") == ""),
            )
            .withColumn(
                "description",
                F.when(F.col("is_missing_description"), F.lit("UNKNOWN PRODUCT")).otherwise(
                    F.col("_raw_desc")
                ),
            )
            .withColumn(
                "_raw_cust",
                F.regexp_replace(F.trim(F.col("CustomerID").cast(T.StringType())), r"\.0$", ""),
            )
            .withColumn(
                "is_guest_customer",
                F.col("_raw_cust").isNull() | (F.col("_raw_cust") == ""),
            )
            .withColumn(
                "customer_id",
                F.when(F.col("is_guest_customer"), F.lit("GUEST")).otherwise(F.col("_raw_cust")),
            )
            .withColumn("country", F.trim(F.col("Country").cast(T.StringType())))
            .withColumn("quantity", F.col("Quantity").cast(T.IntegerType()))
            .withColumn("unit_price", F.round(F.col("UnitPrice").cast(T.DoubleType()), 4))
            .withColumn("invoice_ts", F.to_timestamp(F.col("InvoiceDate")))
            .withColumn("invoice_date", F.to_date(F.col("invoice_ts")))
            .withColumn("invoice_year", F.year(F.col("invoice_ts")))
            .withColumn("invoice_month", F.month(F.col("invoice_ts")))
            .withColumn("invoice_year_month", F.date_format(F.col("invoice_ts"), "yyyy-MM"))
            .withColumn("invoice_day_of_week", F.dayofweek(F.col("invoice_ts")))
            .withColumn("invoice_hour", F.hour(F.col("invoice_ts")))
            .withColumn("is_cancelled", F.upper(F.col("invoice_no")).startswith("C"))
            .withColumn("is_non_product_fee", F.col("stock_code").isin(self.special_codes))
            .withColumn("abs_quantity", F.abs(F.col("quantity")))
            .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 4))
            .withColumn(
                "gross_amount",
                F.when(~F.col("is_cancelled"), F.round(F.col("quantity") * F.col("unit_price"), 4)).otherwise(
                    F.lit(0.0)
                ),
            )
            .withColumn(
                "cancelled_amount",
                F.when(
                    F.col("is_cancelled"), F.round(F.abs(F.col("quantity") * F.col("unit_price")), 4)
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("processed_at_utc", F.current_timestamp())
        )

        # Deterministic primary key per line item, disambiguating multi-line occurrences cleanly
        key_window = Window.partitionBy(
            "invoice_no",
            "stock_code",
            "invoice_ts",
            "quantity",
            "unit_price",
            "customer_id",
            "country",
        ).orderBy(F.col("_source_row_index").asc())

        with_rank = cleaned.withColumn("_occurrence_rank", F.row_number().over(key_window))
        with_pk = with_rank.withColumn(
            "transaction_line_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("invoice_no"),
                    F.col("stock_code"),
                    F.date_format(F.col("invoice_ts"), "yyyy-MM-dd HH:mm:ss"),
                    F.col("quantity").cast(T.StringType()),
                    F.format_number(F.col("unit_price"), 4),
                    F.col("customer_id"),
                    F.col("country"),
                    F.col("_occurrence_rank").cast(T.StringType()),
                ),
                256,
            ),
        )

        selected_cols = [
            "transaction_line_id",
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "abs_quantity",
            "unit_price",
            "line_total",
            "gross_amount",
            "cancelled_amount",
            "invoice_ts",
            "invoice_date",
            "invoice_year",
            "invoice_month",
            "invoice_day_of_week",
            "invoice_hour",
            "customer_id",
            "is_guest_customer",
            "is_missing_description",
            "is_cancelled",
            "is_non_product_fee",
            "country",
            "processed_at_utc",
            "invoice_year_month",
            "batch_id",
        ]
        return with_pk.select(*selected_cols)

    def run_batch(self, valid_parquet_path: Path, batch_id: str, build_aggregates: bool = True) -> SparkETLResult:
        """Transform a Silver Parquet batch and write partitioned Gold Parquet + optional aggregates."""
        silver_df = self.spark.read.parquet(str(valid_parquet_path))
        curated_batch_df = self.transform_silver_df(silver_df, batch_id=batch_id).cache()
        curated_count = curated_batch_df.count()

        curated_tx_dir = self.lake_manager.curated_transactions_dir()

        # Dynamic partition overwrite ensures rerunning a batch replaces only its own (invoice_year_month, batch_id) partitions
        (
            curated_batch_df.write.mode("overwrite")
            .partitionBy("invoice_year_month", "batch_id")
            .parquet(str(curated_tx_dir))
        )
        curated_batch_df.unpersist()

        monthly_rows = 0
        product_rows = 0
        customer_rows = 0
        if build_aggregates:
            monthly_rows, product_rows, customer_rows = self.rebuild_gold_aggregates()

        self.checkpoint_store.update_batch_stage(
            batch_id=batch_id,
            status="TRANSFORMED",
            record_count_curated=int(curated_count),
        )
        self.checkpoint_store.record_step_checkpoint(batch_id, "transformation", "COMPLETED")

        logger.info(
            "PySpark batch transformation completed",
            extra={
                "batch_id": batch_id,
                "curated_row_count": curated_count,
                "curated_path": str(curated_tx_dir),
            },
        )
        return SparkETLResult(
            batch_id=batch_id,
            curated_row_count=int(curated_count),
            curated_transactions_path=curated_tx_dir,
            monthly_summary_rows=monthly_rows,
            product_summary_rows=product_rows,
            customer_summary_rows=customer_rows,
        )

    def rebuild_gold_aggregates(self) -> tuple[int, int, int]:
        """Recompute curated Gold summary Parquet tables across all loaded partitions."""
        curated_tx_dir = self.lake_manager.curated_transactions_dir()
        full_df = self.spark.read.parquet(str(curated_tx_dir)).dropDuplicates(["transaction_line_id"])

        monthly_df = (
            full_df.groupBy("invoice_year_month", "country")
            .agg(
                F.count("*").alias("line_items"),
                F.countDistinct("invoice_no").alias("distinct_invoices"),
                F.countDistinct(F.when(~F.col("is_guest_customer"), F.col("customer_id"))).alias(
                    "identified_customers"
                ),
                F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
                F.round(F.sum("cancelled_amount"), 2).alias("cancelled_revenue"),
                F.round(F.sum("line_total"), 2).alias("net_revenue"),
            )
            .orderBy("invoice_year_month", F.desc("net_revenue"))
        )
        monthly_dir = self.lake_manager.curated_aggregates_dir("gold_monthly_country_summary")
        monthly_df.write.mode("overwrite").parquet(str(monthly_dir))
        monthly_count = monthly_df.count()

        product_df = (
            full_df.filter(~F.col("is_non_product_fee"))
            .groupBy("stock_code")
            .agg(
                F.first("description").alias("sample_description"),
                F.sum(F.when(~F.col("is_cancelled"), F.col("quantity")).otherwise(0)).alias("units_sold"),
                F.sum(F.when(F.col("is_cancelled"), F.col("abs_quantity")).otherwise(0)).alias(
                    "units_cancelled"
                ),
                F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
                F.round(F.sum("cancelled_amount"), 2).alias("cancelled_revenue"),
                F.round(F.sum("line_total"), 2).alias("net_revenue"),
                F.countDistinct("invoice_no").alias("distinct_invoices"),
            )
            .orderBy(F.desc("net_revenue"))
        )
        product_dir = self.lake_manager.curated_aggregates_dir("gold_product_performance")
        product_df.write.mode("overwrite").parquet(str(product_dir))
        product_count = product_df.count()

        customer_df = (
            full_df.filter(~F.col("is_guest_customer"))
            .groupBy("customer_id")
            .agg(
                F.first("country").alias("primary_country"),
                F.min("invoice_ts").alias("first_purchase_ts"),
                F.max("invoice_ts").alias("last_purchase_ts"),
                F.countDistinct(F.when(~F.col("is_cancelled"), F.col("invoice_no"))).alias(
                    "completed_orders"
                ),
                F.countDistinct(F.when(F.col("is_cancelled"), F.col("invoice_no"))).alias(
                    "cancelled_orders"
                ),
                F.round(F.sum("gross_amount"), 2).alias("gross_spend"),
                F.round(F.sum("line_total"), 2).alias("net_spend"),
            )
            .orderBy(F.desc("net_spend"))
        )
        customer_dir = self.lake_manager.curated_aggregates_dir("gold_customer_summary")
        customer_df.write.mode("overwrite").parquet(str(customer_dir))
        customer_count = customer_df.count()

        return int(monthly_count), int(product_count), int(customer_count)
