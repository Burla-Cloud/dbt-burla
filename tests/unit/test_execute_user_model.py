"""Unit tests for `_execute_user_model` - the closure that runs on the worker."""

from __future__ import annotations

import pandas as pd
import pytest

from dbt.adapters.burla.errors import BurlaSubmissionError
from dbt.adapters.burla.python_submissions import _execute_user_model
from dbt.adapters.burla.warehouses.base import RelationRef

pytestmark = pytest.mark.unit


def test_runs_model_and_returns_dataframe(
    compiled_code_single_ref: str, sample_df: pd.DataFrame
) -> None:
    upstream_data = {'"db"."main"."stg_orders"': sample_df}
    target = RelationRef("db", "main", "py_model")

    result = _execute_user_model(
        compiled_code=compiled_code_single_ref.lstrip(),
        upstream_data=upstream_data,
        target=target,
        is_incremental=False,
    )
    pd.testing.assert_frame_equal(result.reset_index(drop=True), sample_df)


def test_raises_when_model_missing() -> None:
    code = "def not_a_model(dbt, session): pass\nclass dbtObj:\n    def __init__(self, f): pass\n"
    with pytest.raises(BurlaSubmissionError, match="missing required"):
        _execute_user_model(
            compiled_code=code,
            upstream_data={},
            target=RelationRef(None, "main", "m"),
            is_incremental=False,
        )


def test_raises_when_dbt_obj_missing() -> None:
    code = "def model(dbt, session):\n    return None\n"
    with pytest.raises(BurlaSubmissionError, match="missing required"):
        _execute_user_model(
            compiled_code=code,
            upstream_data={},
            target=RelationRef(None, "main", "m"),
            is_incremental=False,
        )


def test_raises_on_compile_error() -> None:
    with pytest.raises(BurlaSubmissionError, match="failed to compile"):
        _execute_user_model(
            compiled_code="this is not { valid python",
            upstream_data={},
            target=RelationRef(None, "main", "m"),
            is_incremental=False,
        )


def test_wraps_user_exception() -> None:
    code = """
def model(dbt, session):
    raise ValueError("boom")


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
    with pytest.raises(BurlaSubmissionError, match="boom"):
        _execute_user_model(
            compiled_code=code,
            upstream_data={},
            target=RelationRef(None, "main", "m"),
            is_incremental=False,
        )


def test_overrides_is_incremental(sample_df: pd.DataFrame) -> None:
    code = """
def model(dbt, session):
    df = dbt.ref("stg_orders")
    df = df.assign(is_incremental=dbt.is_incremental)
    return df


def ref(*args, **kwargs):
    refs = {"stg_orders": "\\"db\\".\\"main\\".\\"stg_orders\\""}
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs['.'.join(args)])


def source(*args, dbt_load_df_function):
    sources = {}
    return dbt_load_df_function(sources['.'.join(args)])


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *a: source(*a, dbt_load_df_function=load_df_function)
        self.ref = lambda *a, **kw: ref(*a, **kw, dbt_load_df_function=load_df_function)
        self.is_incremental = False
"""
    result = _execute_user_model(
        compiled_code=code,
        upstream_data={'"db"."main"."stg_orders"': sample_df},
        target=RelationRef("db", "main", "py_model"),
        is_incremental=True,
    )
    assert result["is_incremental"].unique().tolist() == [True]


def test_raises_on_missing_upstream(compiled_code_single_ref: str) -> None:
    with pytest.raises(BurlaSubmissionError, match="not pre-loaded"):
        _execute_user_model(
            compiled_code=compiled_code_single_ref.lstrip(),
            upstream_data={},  # empty, so the ref lookup fails
            target=RelationRef("db", "main", "py_model"),
            is_incremental=False,
        )
