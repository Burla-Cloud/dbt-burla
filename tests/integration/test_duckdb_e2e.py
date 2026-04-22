"""End-to-end integration tests running real `dbt run` against DuckDB + Burla-fake."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

pytestmark = pytest.mark.duckdb


def _db_path(profiles_dir: Path) -> Path:
    text = (profiles_dir / "profiles.yml").read_text()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("path:"):
            return Path(line.split(":", 1)[1].strip().strip('"'))
    raise AssertionError("no path in profiles.yml")


def test_debug(run_dbt: Any) -> None:
    result = run_dbt("debug")
    assert result.success, result.exception


def test_run_pipeline_end_to_end(run_dbt: Any, duckdb_profiles_dir: Path) -> None:
    result = run_dbt("run")
    assert result.success, f"dbt run failed: {result.exception}\n" + "\n".join(
        str(r) for r in (result.result or [])
    )

    db_path = _db_path(duckdb_profiles_dir)
    con = duckdb.connect(str(db_path))
    try:
        # stg_orders was a view model
        stg_rows = con.execute("select count(*) from main.stg_orders").fetchone()
        assert stg_rows == (4,)

        # enriched_orders is the Python table model
        enriched = con.execute(
            "select id, customer_name, amount, amount_cents, is_large, greeting "
            "from main.enriched_orders order by id"
        ).fetchall()
        assert len(enriched) == 4
        assert enriched[0] == (1, "alice", 120.50, 12050, True, "Hello, alice!")
        assert enriched[3] == (4, "dave", 5.00, 500, False, "Hello, dave!")

        # order_summary depends on enriched_orders
        summary = con.execute("select * from main.order_summary").fetchone()
        assert summary is not None
        total_amount, total_cents, large_count, row_count = summary
        assert row_count == 4
        assert large_count == 2  # alice (120.50) and carol (500)
        assert round(total_amount, 2) == 665.50
        assert total_cents == 66550
    finally:
        con.close()


def test_run_is_idempotent(run_dbt: Any, duckdb_profiles_dir: Path) -> None:
    first = run_dbt("run")
    assert first.success
    second = run_dbt("run")
    assert second.success
    db_path = _db_path(duckdb_profiles_dir)
    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("select count(*) from main.enriched_orders").fetchone()
        assert rows == (4,)
    finally:
        con.close()


def test_dbt_test_passes(run_dbt: Any) -> None:
    run_result = run_dbt("run")
    assert run_result.success
    test_result = run_dbt("test")
    assert test_result.success, f"dbt test failed: {test_result.exception}\n" + "\n".join(
        str(r) for r in (test_result.result or [])
    )


def test_changed_python_model_rebuilds(
    run_dbt: Any, duckdb_project: Path, duckdb_profiles_dir: Path
) -> None:
    assert run_dbt("run").success

    # Modify the python model to emit a different column set
    py_file = duckdb_project / "models" / "enriched_orders.py"
    original = py_file.read_text()
    modified = original.replace(
        'greeting=orders["customer_name"].map(lambda n: f"Hello, {n}!"),',
        'greeting=orders["customer_name"].map(lambda n: f"Hi there, {n}!"),',
    )
    py_file.write_text(modified)

    assert run_dbt("run", "--select", "enriched_orders").success

    db_path = _db_path(duckdb_profiles_dir)
    con = duckdb.connect(str(db_path))
    try:
        greetings = [
            r[0]
            for r in con.execute("select greeting from main.enriched_orders order by id").fetchall()
        ]
    finally:
        con.close()
    assert greetings[0].startswith("Hi there,")
