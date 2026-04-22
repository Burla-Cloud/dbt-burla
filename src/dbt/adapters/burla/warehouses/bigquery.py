"""BigQuery warehouse backend.

Reads via the BigQuery Storage API through ``to_dataframe()``, writes via
``client.load_table_from_dataframe`` which uploads Parquet.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbt.adapters.burla.errors import BurlaImportError
from dbt.adapters.burla.warehouses.base import RelationRef, WarehouseBackend, WriteMode

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["BigQueryBackend"]


class BigQueryBackend(WarehouseBackend):
    def __init__(self, handle: Any) -> None:
        self._handle = handle

    @classmethod
    def from_connection_handle(cls, handle: Any, credentials: Any) -> BigQueryBackend:
        del credentials
        return cls(handle)

    @property
    def _client(self) -> Any:
        client = getattr(self._handle, "client", None) or getattr(self._handle, "_client", None)
        if client is None:
            return self._handle
        return client

    def read_as_dataframe(self, relation: RelationRef) -> pd.DataFrame:
        try:
            import pandas  # noqa: F401
        except ImportError as exc:
            raise BurlaImportError.for_extra("bigquery", "pandas") from exc
        client = self._client
        fq = relation.render_unquoted()
        query_job = client.query(f"select * from `{fq}`")
        result = query_job.result()
        return result.to_dataframe(create_bqstorage_client=True)

    def write_from_dataframe(
        self,
        df: pd.DataFrame,
        relation: RelationRef,
        *,
        mode: WriteMode = "replace",
    ) -> None:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise BurlaImportError.for_extra("bigquery", "google-cloud-bigquery") from exc

        client = self._client
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if mode == "replace"
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        dest = relation.render_unquoted()
        job = client.load_table_from_dataframe(df, dest, job_config=job_config)
        job.result()

    def drop_if_exists(self, relation: RelationRef) -> None:
        client = self._client
        fq = relation.render_unquoted()
        client.query(f"drop table if exists `{fq}`").result()
