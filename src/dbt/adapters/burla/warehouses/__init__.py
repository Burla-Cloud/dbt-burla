"""Warehouse I/O backends used by `BurlaPythonJobHelper` for DataFrame transfer."""

from __future__ import annotations

from dbt.adapters.burla.warehouses.base import RelationRef, WarehouseBackend

__all__ = ["RelationRef", "WarehouseBackend", "get_backend_for_adapter"]


def get_backend_for_adapter(adapter_type: str) -> type[WarehouseBackend]:
    """Return the warehouse backend class for an adapter `type()`."""
    if adapter_type == "burla_duckdb":
        from dbt.adapters.burla.warehouses.duckdb import DuckDBBackend

        return DuckDBBackend
    if adapter_type == "burla_snowflake":
        from dbt.adapters.burla.warehouses.snowflake import SnowflakeBackend

        return SnowflakeBackend
    if adapter_type == "burla_bigquery":
        from dbt.adapters.burla.warehouses.bigquery import BigQueryBackend

        return BigQueryBackend
    raise ValueError(f"No Burla warehouse backend registered for adapter type {adapter_type!r}")
