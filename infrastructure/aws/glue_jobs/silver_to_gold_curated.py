"""AWS Glue 4.0 PySpark Job: Silver Parquet -> Curated Partitioned Gold Parquet + Glue Catalog."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BATCH_ID", "S3_PROCESSED_URI", "S3_CURATED_URI"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SPECIAL_NON_PRODUCT_CODES = [
    "POST",
    "D",
    "M",
    "BANK CHARGES",
    "PADS",
    "DOT",
    "CRUK",
    "AMAZONFEE",
    "S",
]

silver_df = spark.read.parquet(args["S3_PROCESSED_URI"])

cleaned = (
    silver_df.withColumn("invoice_no", F.trim(F.col("InvoiceNo").cast(T.StringType())))
    .withColumn("stock_code", F.upper(F.trim(F.col("StockCode").cast(T.StringType()))))
    .withColumn("_raw_desc", F.trim(F.col("Description").cast(T.StringType())))
    .withColumn("is_missing_description", F.col("_raw_desc").isNull() | (F.col("_raw_desc") == ""))
    .withColumn(
        "description",
        F.when(F.col("is_missing_description"), F.lit("UNKNOWN PRODUCT")).otherwise(F.col("_raw_desc")),
    )
    .withColumn("_raw_cust", F.regexp_replace(F.trim(F.col("CustomerID").cast(T.StringType())), r"\.0$", ""))
    .withColumn("is_guest_customer", F.col("_raw_cust").isNull() | (F.col("_raw_cust") == ""))
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
    .withColumn("is_non_product_fee", F.col("stock_code").isin(SPECIAL_NON_PRODUCT_CODES))
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
        F.when(F.col("is_cancelled"), F.round(F.abs(F.col("quantity") * F.col("unit_price")), 4)).otherwise(
            F.lit(0.0)
        ),
    )
    .withColumn("batch_id", F.lit(args["BATCH_ID"]))
    .withColumn("processed_at_utc", F.current_timestamp())
)

key_window = Window.partitionBy(
    "invoice_no", "stock_code", "invoice_ts", "quantity", "unit_price", "customer_id", "country"
).orderBy(F.col("_source_row_index").asc())

curated_df = (
    cleaned.withColumn("_occurrence_rank", F.row_number().over(key_window))
    .withColumn(
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
)

curated_df.write.mode("overwrite").partitionBy("invoice_year_month", "batch_id").parquet(
    args["S3_CURATED_URI"]
)

job.commit()
