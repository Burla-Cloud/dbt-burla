"""Unit tests for AST-based extraction of refs/sources maps."""

from __future__ import annotations

import pytest

from dbt.adapters.burla.python_submissions import BurlaPythonJobHelper

pytestmark = pytest.mark.unit


def test_extracts_refs(compiled_code_single_ref: str) -> None:
    refs, sources = BurlaPythonJobHelper._extract_relation_maps(compiled_code_single_ref)
    assert refs == {"stg_orders": '"db"."main"."stg_orders"'}
    assert sources == {}


def test_extracts_sources(compiled_code_with_source: str) -> None:
    refs, sources = BurlaPythonJobHelper._extract_relation_maps(compiled_code_with_source)
    assert refs == {}
    assert sources == {"raw.orders": '"db"."raw"."orders"'}


def test_extracts_both_refs_and_sources() -> None:
    code = """
def model(dbt, session):
    a = dbt.ref("upstream")
    b = dbt.source("raw", "events")
    return a.merge(b, on="id")


def ref(*args, **kwargs):
    refs = {"upstream": "\\"db\\".\\"main\\".\\"upstream\\"", "other": "\\"db\\".\\"main\\".\\"other\\""}
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs['.'.join(args)])


def source(*args, dbt_load_df_function):
    sources = {"raw.events": "\\"db\\".\\"raw\\".\\"events\\""}
    return dbt_load_df_function(sources['.'.join(args)])
"""
    refs, sources = BurlaPythonJobHelper._extract_relation_maps(code)
    assert refs == {
        "upstream": '"db"."main"."upstream"',
        "other": '"db"."main"."other"',
    }
    assert sources == {"raw.events": '"db"."raw"."events"'}


def test_returns_empty_when_no_functions() -> None:
    refs, sources = BurlaPythonJobHelper._extract_relation_maps(
        "def model(dbt, session):\n    return None\n"
    )
    assert refs == {}
    assert sources == {}


def test_handles_malformed_code_gracefully() -> None:
    # AST.parse on truly malformed code raises; we test that valid Python with
    # unexpected structure doesn't crash.
    code = """
def ref(*args, **kwargs):
    # not an assignment to `refs = {...}` - should be ignored
    pass
"""
    refs, sources = BurlaPythonJobHelper._extract_relation_maps(code)
    assert refs == {}
    assert sources == {}


def test_raises_on_syntax_error() -> None:
    with pytest.raises(SyntaxError):
        BurlaPythonJobHelper._extract_relation_maps("def model(:")
