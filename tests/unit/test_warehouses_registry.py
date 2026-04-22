"""Unit tests for `get_backend_for_adapter` dispatch."""

from __future__ import annotations

import pytest

from dbt.adapters.burla.warehouses import (
    WarehouseBackend,
    get_backend_for_adapter,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "adapter_type",
    ["burla_duckdb", "burla_snowflake", "burla_bigquery"],
)
def test_known_adapter_types(adapter_type: str) -> None:
    backend_cls = get_backend_for_adapter(adapter_type)
    assert issubclass(backend_cls, WarehouseBackend)


def test_unknown_adapter_type_raises() -> None:
    with pytest.raises(ValueError) as excinfo:
        get_backend_for_adapter("some_unknown_adapter")
    assert "some_unknown_adapter" in str(excinfo.value)
