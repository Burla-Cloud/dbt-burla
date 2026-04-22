"""Snowflake warehouse backend.

Uses ``snowflake-connector-python``'s ``write_pandas`` for uploads and
``fetch_pandas_all`` for downloads. Writes go through a temp table swap so the
target relation is never empty/partial during a replace.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dbt.adapters.burla.errors import BurlaImportError
from dbt.adapters.burla.warehouses.base import RelationRef, WarehouseBackend, WriteMode

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["SnowflakeBackend"]


class SnowflakeBackend(WarehouseBackend):
    def __init__(self, handle: Any) -> None:
        self._handle = handle

    @classmethod
    def from_connection_handle(cls, handle: Any, credentials: Any) -> SnowflakeBackend:
        del credentials
        return cls(handle)

    def _cursor(self) -> Any:
        return self._handle.cursor()

    def read_as_dataframe(self, relation: RelationRef) -> pd.DataFrame:
        try:
            import pandas  # noqa: F401
        except ImportError as exc:
            raise BurlaImportError.for_extra("snowflake", "pandas") from exc
        with self._cursor() as cur:
            cur.execute(f"select * from {relation.render()}")
            return cur.fetch_pandas_all()

    def write_from_dataframe(
        self,
        df: pd.DataFrame,
        relation: RelationRef,
        *,
        mode: WriteMode = "replace",
    ) -> None:
        try:
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError as exc:
            raise BurlaImportError.for_extra("snowflake", "snowflake-connector-python") from exc

        staging_name = f"_BURLA_STG_{relation.identifier.upper()}_{uuid4().hex[:8]}"
        write_pandas(
            self._handle,
            df,
            table_name=staging_name,
            database=relation.database,
            schema=relation.schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        staging_ref = RelationRef(relation.database, relation.schema, staging_name)
        try:
            with self._cursor() as cur:
                if mode == "replace":
                    cur.execute(
                        f"create or replace table {relation.render()} as "
                        f"select * from {staging_ref.render()}"
                    )
                elif mode == "append":
                    cur.execute(
                        f"create table if not exists {relation.render()} as "
                        f"select * from {staging_ref.render()} where 1=0"
                    )
                    cur.execute(
                        f"insert into {relation.render()} select * from {staging_ref.render()}"
                    )
                else:  # pragma: no cover
                    raise ValueError(f"unknown write mode {mode!r}")
        finally:
            with self._cursor() as cur:
                cur.execute(f"drop table if exists {staging_ref.render()}")

    def drop_if_exists(self, relation: RelationRef) -> None:
        with self._cursor() as cur:
            cur.execute(f"drop table if exists {relation.render()}")
