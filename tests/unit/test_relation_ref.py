"""Unit tests for `RelationRef` and relation-string parsing."""

from __future__ import annotations

import pytest

from dbt.adapters.burla.python_submissions import _parse_relation_string
from dbt.adapters.burla.warehouses.base import RelationRef

pytestmark = pytest.mark.unit


class TestRelationRef:
    def test_from_parsed_model(self) -> None:
        parsed = {
            "database": "db",
            "schema": "main",
            "alias": "my_model",
            "name": "my_model",
        }
        ref = RelationRef.from_parsed_model(parsed)
        assert ref.database == "db"
        assert ref.schema == "main"
        assert ref.identifier == "my_model"

    def test_from_parsed_model_without_alias_uses_name(self) -> None:
        parsed = {"database": "db", "schema": "main", "name": "my_model"}
        ref = RelationRef.from_parsed_model(parsed)
        assert ref.identifier == "my_model"

    def test_from_parsed_model_without_database(self) -> None:
        parsed = {"schema": "main", "name": "my_model"}
        ref = RelationRef.from_parsed_model(parsed)
        assert ref.database is None
        assert ref.schema == "main"

    def test_render_fully_qualified(self) -> None:
        ref = RelationRef(database="db", schema="main", identifier="t")
        assert ref.render() == '"db"."main"."t"'

    def test_render_with_custom_quote(self) -> None:
        ref = RelationRef(database="db", schema="main", identifier="t")
        assert ref.render(quote="`") == "`db`.`main`.`t`"

    def test_render_unquoted(self) -> None:
        ref = RelationRef(database="db", schema="main", identifier="t")
        assert ref.render_unquoted() == "db.main.t"

    def test_render_drops_nones(self) -> None:
        ref = RelationRef(database=None, schema="main", identifier="t")
        assert ref.render() == '"main"."t"'
        assert ref.render_unquoted() == "main.t"

    def test_frozen(self) -> None:
        import dataclasses

        ref = RelationRef(database="db", schema="main", identifier="t")
        with pytest.raises(dataclasses.FrozenInstanceError):
            ref.database = "other"  # type: ignore[misc]


class TestParseRelationString:
    def test_fully_qualified_with_quotes(self) -> None:
        ref = _parse_relation_string('"db"."main"."stg_orders"')
        assert ref == RelationRef("db", "main", "stg_orders")

    def test_fully_qualified_unquoted(self) -> None:
        ref = _parse_relation_string("db.main.stg_orders")
        assert ref == RelationRef("db", "main", "stg_orders")

    def test_mixed_quoting(self) -> None:
        ref = _parse_relation_string('"db".main."stg_orders"')
        assert ref == RelationRef("db", "main", "stg_orders")

    def test_bigquery_backticks(self) -> None:
        ref = _parse_relation_string("`my-project`.`dataset`.`table`")
        assert ref == RelationRef("my-project", "dataset", "table")

    def test_two_parts(self) -> None:
        ref = _parse_relation_string("main.stg_orders")
        assert ref == RelationRef(None, "main", "stg_orders")

    def test_single_identifier(self) -> None:
        ref = _parse_relation_string("stg_orders")
        assert ref == RelationRef(None, None, "stg_orders")

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_relation_string("")

    def test_too_many_parts_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_relation_string("a.b.c.d.e")
