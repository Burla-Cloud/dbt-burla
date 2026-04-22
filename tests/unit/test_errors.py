"""Unit tests for `dbt.adapters.burla.errors`."""

from __future__ import annotations

import pytest

from dbt.adapters.burla.errors import (
    BurlaAdapterError,
    BurlaConfigError,
    BurlaImportError,
    BurlaResultError,
    BurlaSubmissionError,
)

pytestmark = pytest.mark.unit


def test_all_errors_subclass_base() -> None:
    for err in (
        BurlaConfigError,
        BurlaImportError,
        BurlaResultError,
        BurlaSubmissionError,
    ):
        assert issubclass(err, BurlaAdapterError)
        assert issubclass(err, Exception)


def test_for_extra_message_mentions_pip_install() -> None:
    err = BurlaImportError.for_extra("snowflake", "snowflake-connector-python")
    message = str(err)
    assert "snowflake-connector-python" in message
    assert "dbt-burla[snowflake]" in message
    assert "pip install" in message
