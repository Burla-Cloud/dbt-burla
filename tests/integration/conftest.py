"""Shared fixtures for integration tests."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

PROJECTS_DIR = Path(__file__).parent / "projects"


@pytest.fixture
def duckdb_project(tmp_path: Path) -> Path:
    """Copy the duckdb_basic fixture project into a tmpdir for isolation."""
    src = PROJECTS_DIR / "duckdb_basic"
    dest = tmp_path / "project"
    shutil.copytree(src, dest)
    return dest


@pytest.fixture
def duckdb_profiles_dir(tmp_path: Path) -> Path:
    """A `profiles.yml` directory pointing at a tmpdir-local DuckDB file."""
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    db_path = tmp_path / "warehouse.duckdb"
    (profile_dir / "profiles.yml").write_text(
        f"""
burla_duckdb_basic:
  target: dev
  outputs:
    dev:
      type: burla_duckdb
      path: "{db_path}"
      schema: main
      burla_fake: true
""".strip()
    )
    return profile_dir


@pytest.fixture
def dbt_runner_runner() -> Any:
    from dbt.cli.main import dbtRunner

    return dbtRunner()


def _require_env(*names: str) -> dict[str, str]:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        pytest.skip(f"Missing required env vars: {', '.join(missing)}")
    return {n: os.environ[n] for n in names}


@pytest.fixture
def snowflake_env() -> dict[str, str]:
    return _require_env(
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_WAREHOUSE",
    )


@pytest.fixture
def bigquery_env() -> dict[str, str]:
    return _require_env("BIGQUERY_PROJECT")


@pytest.fixture
def duckdb_db_path(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


@pytest.fixture
def run_dbt(duckdb_project: Path, duckdb_profiles_dir: Path) -> Iterator[Any]:
    """Return a callable that invokes dbt against the fixture project."""
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()

    def _run(*args: str) -> Any:
        return runner.invoke(
            [
                *args,
                "--project-dir",
                str(duckdb_project),
                "--profiles-dir",
                str(duckdb_profiles_dir),
            ]
        )

    yield _run
