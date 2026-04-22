"""Connection manager retagged as `burla_bigquery`."""

from __future__ import annotations

from dbt.adapters.bigquery.connections import BigQueryConnectionManager

__all__ = ["BurlaBigQueryConnectionManager"]


class BurlaBigQueryConnectionManager(BigQueryConnectionManager):
    TYPE = "burla_bigquery"
