"""Shared pytest fixtures for isolated unit and integration testing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.common.config import AppConfig, EnvSettings, load_config
from src.transformation.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Shared local SparkSession for PySpark tests."""
    base_cfg = load_config()
    spark = create_spark_session(
        project_root=base_cfg.project_root,
        app_name="RetailPlatformTests",
        master="local[2]",
        driver_memory="1g",
        shuffle_partitions=2,
        timezone="UTC",
    )
    yield spark
    spark.stop()


@pytest.fixture()
def test_config(tmp_path: Path) -> AppConfig:
    """Return an AppConfig isolated to a temporary data directory."""
    base_cfg = load_config()
    env = EnvSettings(
        DATA_ROOT=str(tmp_path / "data"),
        WAREHOUSE_DB_PATH=str(tmp_path / "data" / "warehouse" / "test_warehouse.duckdb"),
        CHECKPOINT_DB_PATH=str(tmp_path / "data" / "state" / "test_state.sqlite"),
        LOG_FORMAT="console",
    )
    return AppConfig(
        project_root=base_cfg.project_root,
        env=env,
        yaml_config=base_cfg.yaml_config,
    )


@pytest.fixture()
def sample_batch_df() -> pd.DataFrame:
    """Realistic retail dataframe exercising valid rows, cancellations, guests, duplicates, and DQ defects."""
    return pd.DataFrame(
        [
            # 1. Valid normal row
            {
                "InvoiceNo": "536365",
                "StockCode": "85123A",
                "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
                "Quantity": 6,
                "InvoiceDate": "2011-11-25 08:26:00",
                "UnitPrice": 2.55,
                "CustomerID": "17850",
                "Country": "United Kingdom",
            },
            # 2. Exact duplicate of row 1 (should be quarantined as DQ_EXACT_DUPLICATE)
            {
                "InvoiceNo": "536365",
                "StockCode": "85123A",
                "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
                "Quantity": 6,
                "InvoiceDate": "2011-11-25 08:26:00",
                "UnitPrice": 2.55,
                "CustomerID": "17850",
                "Country": "United Kingdom",
            },
            # 3. Valid cancellation (retained in Silver/Gold with is_cancelled=True)
            {
                "InvoiceNo": "C536379",
                "StockCode": "D",
                "Description": "Discount",
                "Quantity": -1,
                "InvoiceDate": "2011-11-25 09:41:00",
                "UnitPrice": 27.50,
                "CustomerID": "14527",
                "Country": "United Kingdom",
            },
            # 4. Valid guest checkout (missing CustomerID & Description)
            {
                "InvoiceNo": "536544",
                "StockCode": "22423",
                "Description": None,
                "Quantity": 2,
                "InvoiceDate": "2011-11-25 14:32:00",
                "UnitPrice": 12.75,
                "CustomerID": None,
                "Country": "France",
            },
            # 5. Rejected: zero price
            {
                "InvoiceNo": "536545",
                "StockCode": "22424",
                "Description": "FREE SAMPLE",
                "Quantity": 1,
                "InvoiceDate": "2011-11-25 14:40:00",
                "UnitPrice": 0.0,
                "CustomerID": "17850",
                "Country": "United Kingdom",
            },
            # 6. Rejected: negative quantity without C prefix
            {
                "InvoiceNo": "536546",
                "StockCode": "22425",
                "Description": "DAMAGED",
                "Quantity": -5,
                "InvoiceDate": "2011-11-25 14:45:00",
                "UnitPrice": 1.25,
                "CustomerID": "17850",
                "Country": "United Kingdom",
            },
            # 7. Rejected: non-standard bad-debt invoice
            {
                "InvoiceNo": "A563186",
                "StockCode": "B",
                "Description": "Adjust bad debt",
                "Quantity": 1,
                "InvoiceDate": "2011-11-25 15:00:00",
                "UnitPrice": -100.0,
                "CustomerID": None,
                "Country": "United Kingdom",
            },
        ]
    )


@pytest.fixture()
def sample_batch_csv(tmp_path: Path, sample_batch_df: pd.DataFrame) -> Path:
    path = tmp_path / "sample_batch.csv"
    sample_batch_df.to_csv(path, index=False)
    return path
