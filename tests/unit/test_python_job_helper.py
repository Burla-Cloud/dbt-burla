"""Unit tests for `BurlaPythonJobHelper.submit` end-to-end with mocks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dbt.adapters.burla.errors import BurlaResultError, BurlaSubmissionError
from dbt.adapters.burla.python_submissions import BurlaPythonJobHelper
from dbt.adapters.burla.warehouses.base import RelationRef, WarehouseBackend, WriteMode

pytestmark = pytest.mark.unit


@dataclass
class _StubCredentials:
    burla_cluster_url: str | None = None
    burla_default_workers: int = 16
    burla_default_cpus_per_worker: int = 1
    burla_default_ram_per_worker: int | None = None
    burla_default_image: str | None = None
    burla_default_timeout_s: int = 3600
    burla_fake: bool = True


class _FakeBackend(WarehouseBackend):
    def __init__(self) -> None:
        self.reads: list[RelationRef] = []
        self.writes: list[tuple[RelationRef, pd.DataFrame, WriteMode]] = []
        self.dropped: list[RelationRef] = []
        self._upstream: dict[str, pd.DataFrame] = {}

    @classmethod
    def from_connection_handle(cls, handle: Any, credentials: Any) -> _FakeBackend:
        return cls()

    def seed(self, relation: RelationRef, df: pd.DataFrame) -> None:
        # Make read_as_dataframe return this df
        self._upstream[relation.render_unquoted()] = df

    def read_as_dataframe(self, relation: RelationRef) -> pd.DataFrame:
        self.reads.append(relation)
        return self._upstream.get(relation.render_unquoted(), pd.DataFrame())

    def write_from_dataframe(
        self,
        df: pd.DataFrame,
        relation: RelationRef,
        *,
        mode: WriteMode = "replace",
    ) -> None:
        self.writes.append((relation, df, mode))

    def drop_if_exists(self, relation: RelationRef) -> None:
        self.dropped.append(relation)


@pytest.fixture
def fake_backend(monkeypatch: pytest.MonkeyPatch) -> _FakeBackend:
    """Register `_FakeBackend` for the `burla_duckdb` adapter type."""
    import dbt.adapters.burla.warehouses as warehouses_mod

    instance = _FakeBackend()

    class _FakeBackendSingleton(_FakeBackend):
        @classmethod
        def from_connection_handle(cls, handle: Any, credentials: Any) -> _FakeBackend:
            return instance

    monkeypatch.setattr(
        warehouses_mod,
        "get_backend_for_adapter",
        lambda adapter_type: _FakeBackendSingleton,
    )
    # Also patch the helper's own import path
    import dbt.adapters.burla.python_submissions as helper_mod

    monkeypatch.setattr(
        helper_mod, "get_backend_for_adapter", lambda adapter_type: _FakeBackendSingleton
    )
    return instance


@pytest.fixture
def stub_adapter(fake_backend: _FakeBackend) -> MagicMock:
    adapter = MagicMock()
    adapter.type.return_value = "burla_duckdb"
    adapter.config.credentials = _StubCredentials()
    adapter.connections.get_thread_connection.return_value = MagicMock(handle=MagicMock())
    return adapter


def test_submit_round_trips_dataframe(
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    compiled_code_single_ref: str,
    sample_df: pd.DataFrame,
    parsed_model_table: dict[str, Any],
) -> None:
    fake_backend.seed(RelationRef("db", "main", "stg_orders"), sample_df)

    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model_table)
    result = helper.submit(compiled_code_single_ref)

    assert result["rows"] == 3
    assert len(fake_backend.reads) == 1
    assert fake_backend.reads[0] == RelationRef("db", "main", "stg_orders")
    assert len(fake_backend.writes) == 1
    written_relation, written_df, mode = fake_backend.writes[0]
    assert written_relation == RelationRef("db", "main", "py_model")
    assert mode == "replace"
    pd.testing.assert_frame_equal(written_df.reset_index(drop=True), sample_df)


def test_submit_incremental_uses_append(
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    compiled_code_single_ref: str,
    sample_df: pd.DataFrame,
    parsed_model_incremental: dict[str, Any],
) -> None:
    fake_backend.seed(RelationRef("db", "main", "stg_orders"), sample_df)

    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model_incremental)
    helper.submit(compiled_code_single_ref)

    _, _, mode = fake_backend.writes[0]
    assert mode == "append"


def test_submit_respects_model_config_overrides(
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    compiled_code_single_ref: str,
    sample_df: pd.DataFrame,
    parsed_model_incremental: dict[str, Any],
) -> None:
    fake_backend.seed(RelationRef("db", "main", "stg_orders"), sample_df)

    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model_incremental)
    assert helper.model_config.workers == 100
    assert helper.model_config.cpus_per_worker == 8


def test_submit_rejects_non_dataframe(
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    parsed_model_table: dict[str, Any],
) -> None:
    bad_code = """
def model(dbt, session):
    return {"not": "a dataframe"}


def ref(*args, **kwargs):
    refs = {}
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return None


def source(*args, dbt_load_df_function):
    sources = {}
    return None


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *a: None
        self.ref = lambda *a, **kw: None
        self.is_incremental = False
"""
    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model_table)
    with pytest.raises(BurlaResultError, match="must return a pandas DataFrame"):
        helper.submit(bad_code)


def test_submit_without_adapter_raises() -> None:
    helper = BurlaPythonJobHelper({"alias": "m", "name": "m"}, _StubCredentials())
    with pytest.raises(BurlaSubmissionError, match="without an adapter"):
        helper.submit("def model(dbt, session): return None")


def test_submit_strips_leading_whitespace(
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    compiled_code_single_ref: str,
    sample_df: pd.DataFrame,
    parsed_model_table: dict[str, Any],
) -> None:
    fake_backend.seed(RelationRef("db", "main", "stg_orders"), sample_df)
    # Compiled code already starts with blank indented lines, so submit must
    # lstrip() to avoid IndentationError - this test exercises that path.
    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model_table)
    helper.submit(compiled_code_single_ref)
    assert len(fake_backend.writes) == 1


def test_submit_uses_burla_remote_parallel_map_when_not_fake(
    monkeypatch: pytest.MonkeyPatch,
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    compiled_code_single_ref: str,
    sample_df: pd.DataFrame,
) -> None:
    """When `burla_fake=False`, the helper must route through remote_parallel_map with our kwargs."""
    fake_backend.seed(RelationRef("db", "main", "stg_orders"), sample_df)
    parsed_model = {
        "name": "py_model",
        "alias": "py_model",
        "schema": "main",
        "database": "db",
        "unique_id": "model.test.py_model",
        "config": {
            "materialized": "table",
            "burla_workers": 50,
            "burla_cpus_per_worker": 4,
            "burla_ram_per_worker": 16,
            "burla_image": "my-image:latest",
            "burla_fake": False,
        },
        "refs": [{"name": "stg_orders"}],
        "sources": [],
    }

    calls: list[dict[str, Any]] = []

    def fake_rpm(worker: Any, inputs: Any, **kwargs: Any) -> list[Any]:
        calls.append({"worker": worker, "inputs": inputs, **kwargs})
        # Invoke the worker in-process to produce a real result
        return [worker(i) for i in inputs]

    # Patch the burla client *module* that the helper imports lazily
    import sys
    import types

    fake_burla = types.ModuleType("burla")
    fake_burla.remote_parallel_map = fake_rpm  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "burla", fake_burla)

    # Also set cluster url so we exercise that branch
    stub_adapter.config.credentials.burla_cluster_url = "https://cluster.example.com"

    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model)
    helper.submit(compiled_code_single_ref)

    assert len(calls) == 1
    call = calls[0]
    assert call["inputs"] == [None]
    assert call["func_cpu"] == 4
    assert call["func_ram"] == 16
    assert call["image"] == "my-image:latest"


def test_submit_wraps_burla_exception(
    monkeypatch: pytest.MonkeyPatch,
    stub_adapter: MagicMock,
    fake_backend: _FakeBackend,
    compiled_code_single_ref: str,
    sample_df: pd.DataFrame,
) -> None:
    fake_backend.seed(RelationRef("db", "main", "stg_orders"), sample_df)
    parsed_model = {
        "name": "py_model",
        "alias": "py_model",
        "schema": "main",
        "database": "db",
        "unique_id": "model.test.py_model",
        "config": {"materialized": "table", "burla_fake": False},
        "refs": [{"name": "stg_orders"}],
        "sources": [],
    }

    def fake_rpm(worker: Any, inputs: Any, **kwargs: Any) -> list[Any]:
        raise RuntimeError("cluster unreachable")

    import sys
    import types

    fake_burla = types.ModuleType("burla")
    fake_burla.remote_parallel_map = fake_rpm  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "burla", fake_burla)

    helper = BurlaPythonJobHelper.for_adapter(stub_adapter, parsed_model)
    with pytest.raises(BurlaSubmissionError, match="cluster unreachable"):
        helper.submit(compiled_code_single_ref)
