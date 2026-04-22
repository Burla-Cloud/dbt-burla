"""Unit tests for the DuckDB warehouse backend."""

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from dbt.adapters.burla.warehouses.base import RelationRef
from dbt.adapters.burla.warehouses.duckdb import DuckDBBackend

pytestmark = pytest.mark.unit


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


@pytest.fixture
def backend(con: duckdb.DuckDBPyConnection) -> DuckDBBackend:
    return DuckDBBackend.from_connection_handle(con, credentials=None)


def _seed(con: duckdb.DuckDBPyConnection, schema: str, name: str, df: pd.DataFrame) -> None:
    con.execute(f'create schema if not exists "{schema}"')
    con.register("__seed", df)
    con.execute(f'create table "{schema}"."{name}" as select * from __seed')
    con.unregister("__seed")


def test_read_as_dataframe(backend: DuckDBBackend, con: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
    _seed(con, "main", "users", df)
    result = backend.read_as_dataframe(RelationRef(None, "main", "users"))
    assert list(result.columns) == ["id", "name"]
    assert len(result) == 2


def test_write_from_dataframe_replace(
    backend: DuckDBBackend, con: duckdb.DuckDBPyConnection
) -> None:
    df = pd.DataFrame([{"x": 1}, {"x": 2}])
    target = RelationRef(None, "main", "out_replace")
    backend.write_from_dataframe(df, target, mode="replace")
    rows = con.execute('select count(*) from "main"."out_replace"').fetchone()
    assert rows is not None
    assert rows[0] == 2

    # Replace again with a different DataFrame - old data should be gone.
    backend.write_from_dataframe(pd.DataFrame([{"x": 10}]), target, mode="replace")
    values = con.execute('select x from "main"."out_replace" order by x').fetchall()
    assert values == [(10,)]


def test_write_from_dataframe_append(
    backend: DuckDBBackend, con: duckdb.DuckDBPyConnection
) -> None:
    target = RelationRef(None, "main", "out_append")
    backend.write_from_dataframe(pd.DataFrame([{"x": 1}]), target, mode="append")
    backend.write_from_dataframe(pd.DataFrame([{"x": 2}, {"x": 3}]), target, mode="append")
    values = con.execute('select x from "main"."out_append" order by x').fetchall()
    assert values == [(1,), (2,), (3,)]


def test_drop_if_exists_removes_relation(
    backend: DuckDBBackend, con: duckdb.DuckDBPyConnection
) -> None:
    target = RelationRef(None, "main", "to_drop")
    backend.write_from_dataframe(pd.DataFrame([{"x": 1}]), target)
    assert con.execute('select count(*) from "main"."to_drop"').fetchone() == (1,)
    backend.drop_if_exists(target)
    with pytest.raises(duckdb.CatalogException):
        con.execute('select 1 from "main"."to_drop"')


def test_drop_if_exists_missing_relation_is_noop(
    backend: DuckDBBackend,
) -> None:
    backend.drop_if_exists(RelationRef(None, "main", "does_not_exist"))


def test_write_creates_schema_if_missing(
    backend: DuckDBBackend, con: duckdb.DuckDBPyConnection
) -> None:
    target = RelationRef(None, "brand_new_schema", "x")
    backend.write_from_dataframe(pd.DataFrame([{"n": 1}]), target)
    rows = con.execute('select count(*) from "brand_new_schema"."x"').fetchone()
    assert rows == (1,)
