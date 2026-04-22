"""DuckDB warehouse backend.

DuckDB is in-process, so the connection handle here is a ``duckdb.DuckDBPyConnection``.
We register a DataFrame as a view, then ``CREATE OR REPLACE TABLE`` from it.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from dbt.adapters.burla.errors import BurlaImportError
from dbt.adapters.burla.warehouses.base import RelationRef, WarehouseBackend, WriteMode

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["DuckDBBackend"]


class DuckDBBackend(WarehouseBackend):
    def __init__(self, handle: Any) -> None:
        self._handle = handle

    @classmethod
    def from_connection_handle(cls, handle: Any, credentials: Any) -> DuckDBBackend:
        del credentials
        return cls(handle)

    @property
    def _cursor(self) -> Any:
        cursor = getattr(self._handle, "cursor", None)
        if callable(cursor):
            return cursor()
        return self._handle

    def read_as_dataframe(self, relation: RelationRef) -> pd.DataFrame:
        try:
            import pandas  # noqa: F401
        except ImportError as exc:
            raise BurlaImportError.for_extra("duckdb", "pandas") from exc
        cur = self._cursor
        return cur.execute(f"select * from {relation.render()}").fetch_df()

    def write_from_dataframe(
        self,
        df: pd.DataFrame,
        relation: RelationRef,
        *,
        mode: WriteMode = "replace",
    ) -> None:
        cur = self._cursor
        view_name = f"__burla_tmp_{relation.identifier}"
        cur.register(view_name, df)
        try:
            if relation.schema:
                cur.execute(f'create schema if not exists "{relation.schema}"')
            if mode == "replace":
                cur.execute(
                    f"create or replace table {relation.render()} as select * from {view_name}"
                )
            elif mode == "append":
                cur.execute(
                    f"create table if not exists {relation.render()} as select * from {view_name} where 1=0"
                )
                cur.execute(f"insert into {relation.render()} select * from {view_name}")
            else:  # pragma: no cover - type checker guards this
                raise ValueError(f"unknown write mode {mode!r}")
        finally:
            with contextlib.suppress(Exception):  # unregister is best-effort
                cur.unregister(view_name)

    def drop_if_exists(self, relation: RelationRef) -> None:
        self._cursor.execute(f"drop table if exists {relation.render()}")
