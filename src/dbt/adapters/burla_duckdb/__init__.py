"""dbt plugin registration for the DuckDB-backed Burla adapter."""

from __future__ import annotations

from pathlib import Path

from dbt.adapters.base.plugin import AdapterPlugin

from dbt.adapters.burla.__version__ import __version__
from dbt.adapters.burla_duckdb.credentials import BurlaDuckDBCredentials
from dbt.adapters.burla_duckdb.impl import BurlaDuckDBAdapter

__all__ = ["Plugin", "__version__"]

Plugin = AdapterPlugin(
    adapter=BurlaDuckDBAdapter,  # type: ignore[arg-type]
    credentials=BurlaDuckDBCredentials,
    include_path=str(Path(__file__).resolve().parents[2] / "include" / "burla_duckdb"),
    dependencies=["duckdb"],
    project_name="dbt_burla_duckdb",
)
