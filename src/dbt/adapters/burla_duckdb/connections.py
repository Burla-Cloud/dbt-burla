"""Connection manager retagged as `burla_duckdb`; everything else is inherited."""

from __future__ import annotations

from dbt.adapters.duckdb.connections import DuckDBConnectionManager

__all__ = ["BurlaDuckDBConnectionManager"]


class BurlaDuckDBConnectionManager(DuckDBConnectionManager):
    TYPE = "burla_duckdb"
