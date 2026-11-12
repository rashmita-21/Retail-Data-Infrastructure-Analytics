"""Configuration management combining environment variables, .env, and YAML settings."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def find_project_root() -> Path:
    """Resolve the repository root directory containing config/pipeline_config.yaml."""
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        if (parent / "config" / "pipeline_config.yaml").exists():
            return parent
    return Path.cwd()


class EnvSettings(BaseSettings):
    """Environment-driven settings loaded from OS env or .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pipeline_env: str = Field(default="local", alias="PIPELINE_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")
    data_root: str = Field(default="data", alias="DATA_ROOT")
    warehouse_db_path: str = Field(
        default="data/warehouse/retail_warehouse.duckdb", alias="WAREHOUSE_DB_PATH"
    )
    checkpoint_db_path: str = Field(
        default="data/state/pipeline_state.sqlite", alias="CHECKPOINT_DB_PATH"
    )
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_driver_memory: str = Field(default="2g", alias="SPARK_DRIVER_MEMORY")
    spark_sql_shuffle_partitions: int = Field(default=8, alias="SPARK_SQL_SHUFFLE_PARTITIONS")
    spark_timezone: str = Field(default="UTC", alias="SPARK_TIMEZONE")
    max_retries: int = Field(default=3, alias="MAX_RETRIES")
    retry_backoff_seconds: float = Field(default=2.0, alias="RETRY_BACKOFF_SECONDS")
    uci_online_retail_url: str = Field(
        default="https://archive.ics.uci.edu/static/public/352/online+retail.zip",
        alias="UCI_ONLINE_RETAIL_URL",
    )


class AppConfig(BaseModel):
    """Unified application configuration."""

    project_root: Path
    env: EnvSettings
    yaml_config: dict[str, Any]

    @property
    def raw_zone(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["zones"]["raw"])

    @property
    def processed_zone(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["zones"]["processed"])

    @property
    def rejected_zone(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["zones"]["rejected"])

    @property
    def curated_zone(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["zones"]["curated"])

    @property
    def reports_dir(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["reports_dir"])

    @property
    def state_dir(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["state_dir"])

    @property
    def batches_dir(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["batches_dir"])

    @property
    def source_dir(self) -> Path:
        return self._resolve_path(self.yaml_config["storage"]["source_dir"])

    @property
    def warehouse_db_path(self) -> Path:
        return self._resolve_path(self.env.warehouse_db_path)

    @property
    def checkpoint_db_path(self) -> Path:
        return self._resolve_path(self.env.checkpoint_db_path)

    @property
    def sql_dir(self) -> Path:
        return self.project_root / "sql"

    def _resolve_path(self, raw_path: str) -> Path:
        candidate = Path(raw_path)
        if candidate.is_absolute():
            return candidate
        # Allow overriding DATA_ROOT via environment for isolated test runs
        if self.env.data_root != "data" and str(candidate).startswith("data"):
            rel = candidate.relative_to("data")
            root = Path(self.env.data_root)
            if not root.is_absolute():
                root = self.project_root / root
            return root / rel
        return self.project_root / candidate


def load_config(config_path: Path | None = None, **env_overrides: Any) -> AppConfig:
    """Load configuration from YAML and environment variables."""
    project_root = find_project_root()
    cfg_file = config_path or (project_root / "config" / "pipeline_config.yaml")
    with cfg_file.open("r", encoding="utf-8") as fh:
        yaml_data = yaml.safe_load(fh)

    env_settings = EnvSettings(**env_overrides)
    if os.getenv("UCI_ONLINE_RETAIL_URL"):
        yaml_data["dataset"]["url"] = os.environ["UCI_ONLINE_RETAIL_URL"]

    return AppConfig(project_root=project_root, env=env_settings, yaml_config=yaml_data)
