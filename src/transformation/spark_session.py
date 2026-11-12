"""PySpark session factory with local Windows/Java auto-configuration."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.common.logging_utils import get_logger

logger = get_logger("transformation.spark_session")


def _configure_local_java_and_hadoop(project_root: Path) -> None:
    """Locate portable JDK in .tools/jdk if JAVA_HOME is unset and configure PySpark Python."""
    if not os.environ.get("JAVA_HOME"):
        jdk_root = project_root / ".tools" / "jdk"
        if jdk_root.exists():
            candidates = sorted(jdk_root.glob("jdk*"))
            if candidates:
                java_home = candidates[0].resolve()
                os.environ["JAVA_HOME"] = str(java_home)
                os.environ["PATH"] = f"{java_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
                logger.info("Configured JAVA_HOME from local .tools", extra={"JAVA_HOME": str(java_home)})

    if not os.environ.get("HADOOP_HOME"):
        hadoop_dir = project_root / ".tools" / "hadoop"
        if hadoop_dir.exists():
            os.environ["HADOOP_HOME"] = str(hadoop_dir.resolve())
            os.environ["PATH"] = f"{hadoop_dir / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def create_spark_session(
    project_root: Path,
    app_name: str = "RetailPlatformETL",
    master: str = "local[*]",
    driver_memory: str = "2g",
    shuffle_partitions: int = 8,
    timezone: str = "UTC",
) -> SparkSession:
    """Create or retrieve a configured SparkSession for local or cluster execution."""
    _configure_local_java_and_hadoop(project_root)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
