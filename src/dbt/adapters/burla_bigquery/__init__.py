"""dbt plugin registration for the BigQuery-backed Burla adapter."""

from __future__ import annotations

from pathlib import Path

from dbt.adapters.base.plugin import AdapterPlugin

from dbt.adapters.burla.__version__ import __version__
from dbt.adapters.burla_bigquery.credentials import BurlaBigQueryCredentials
from dbt.adapters.burla_bigquery.impl import BurlaBigQueryAdapter

__all__ = ["Plugin", "__version__"]

Plugin = AdapterPlugin(
    adapter=BurlaBigQueryAdapter,  # type: ignore[arg-type]
    credentials=BurlaBigQueryCredentials,
    include_path=str(Path(__file__).resolve().parents[2] / "include" / "burla_bigquery"),
    dependencies=["bigquery"],
    project_name="dbt_burla_bigquery",
)
