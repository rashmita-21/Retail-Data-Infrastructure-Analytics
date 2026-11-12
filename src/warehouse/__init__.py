"""Dimensional warehouse loading package."""

from src.warehouse.loader import DuckDBWarehouseLoader, WarehouseLoadResult

__all__ = ["DuckDBWarehouseLoader", "WarehouseLoadResult"]
