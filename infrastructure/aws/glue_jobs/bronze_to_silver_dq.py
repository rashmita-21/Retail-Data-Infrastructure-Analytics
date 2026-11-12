"""AWS Glue 4.0 PySpark Job: Bronze CSV -> Silver Parquet + Rejected Quarantine."""

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
    ["JOB_NAME", "BATCH_ID", "S3_INPUT_URI", "S3_PROCESSED_URI", "S3_REJECTED_URI"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_df = (
    spark.read.option("header", "true")
    .option("escape", '"')
    .csv(args["S3_INPUT_URI"])
    .withColumn("_source_row_index", F.monotonically_increasing_id())
)

parsed = (
    raw_df.withColumn("_inv", F.trim(F.col("InvoiceNo")))
    .withColumn("_stock", F.trim(F.col("StockCode")))
    .withColumn("_country", F.trim(F.col("Country")))
    .withColumn("_ts", F.to_timestamp(F.col("InvoiceDate")))
    .withColumn("_qty", F.col("Quantity").cast(T.DoubleType()))
    .withColumn("_price", F.col("UnitPrice").cast(T.DoubleType()))
)

is_cancel = F.upper(F.col("_inv")).startswith("C")
v_missing = (
    F.col("_inv").isNull()
    | (F.col("_inv") == "")
    | F.col("_stock").isNull()
    | (F.col("_stock") == "")
    | F.col("_country").isNull()
    | (F.col("_country") == "")
    | F.col("_ts").isNull()
    | F.col("_qty").isNull()
    | F.col("_price").isNull()
)
v_non_standard_inv = ~F.col("_inv").rlike(r"^[Cc]?\d+$")
v_zero_qty = F.col("_qty") == 0
v_non_pos_price = F.col("_price") <= 0
v_neg_no_cancel = (F.col("_qty") < 0) & (~is_cancel)
v_pos_on_cancel = (F.col("_qty") > 0) & is_cancel

dup_win = Window.partitionBy(
    "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"
).orderBy("_source_row_index")
with_dup = parsed.withColumn("_dup_rank", F.row_number().over(dup_win))
v_exact_dup = F.col("_dup_rank") > 1

flagged = with_dup.withColumn(
    "rejection_reason",
    F.when(v_missing, F.lit("DQ_MISSING_OR_INVALID_TYPE"))
    .when(v_non_standard_inv, F.lit("DQ_NON_STANDARD_INVOICE"))
    .when(v_zero_qty, F.lit("DQ_ZERO_QUANTITY"))
    .when(v_non_pos_price, F.lit("DQ_NON_POSITIVE_UNIT_PRICE"))
    .when(v_neg_no_cancel, F.lit("DQ_NEGATIVE_QTY_WITHOUT_CANCEL"))
    .when(v_pos_on_cancel, F.lit("DQ_CANCEL_WITH_POSITIVE_QTY"))
    .when(v_exact_dup, F.lit("DQ_EXACT_DUPLICATE"))
    .otherwise(F.lit("")),
).withColumn("batch_id", F.lit(args["BATCH_ID"]))

base_cols = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
    "_source_row_index",
    "batch_id",
]

valid_df = flagged.filter(F.col("rejection_reason") == "").select(*base_cols)
rejected_df = flagged.filter(F.col("rejection_reason") != "").select(*base_cols, "rejection_reason")

valid_df.write.mode("overwrite").parquet(args["S3_PROCESSED_URI"])
rejected_df.write.mode("overwrite").parquet(args["S3_REJECTED_URI"])

job.commit()
