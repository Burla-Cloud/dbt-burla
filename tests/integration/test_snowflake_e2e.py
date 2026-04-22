"""Integration tests for `burla_snowflake`. Skipped unless Snowflake creds are present."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.snowflake


@pytest.fixture
def snowflake_profiles_dir(tmp_path: Path, snowflake_env: dict[str, str]) -> Path:
    """Write a profiles.yml targeting Snowflake + burla_fake."""
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    schema = os.environ.get("SNOWFLAKE_SCHEMA", f"DBT_BURLA_CI_{os.getpid()}")
    (profile_dir / "profiles.yml").write_text(
        f"""
burla_snowflake_basic:
  target: dev
  outputs:
    dev:
      type: burla_snowflake
      account: {snowflake_env["SNOWFLAKE_ACCOUNT"]}
      user: {snowflake_env["SNOWFLAKE_USER"]}
      password: {snowflake_env["SNOWFLAKE_PASSWORD"]}
      database: {snowflake_env["SNOWFLAKE_DATABASE"]}
      warehouse: {snowflake_env["SNOWFLAKE_WAREHOUSE"]}
      schema: {schema}
      threads: 1
      burla_fake: true
""".strip()
    )
    return profile_dir


@pytest.fixture
def snowflake_project(tmp_path: Path) -> Path:
    """A minimal Snowflake project with one SQL + one Python model."""
    import shutil

    src = Path(__file__).parent / "projects" / "duckdb_basic"
    dest = tmp_path / "project"
    shutil.copytree(src, dest)
    # Rename the profile
    proj_yml = dest / "dbt_project.yml"
    proj_yml.write_text(proj_yml.read_text().replace("burla_duckdb_basic", "burla_snowflake_basic"))
    return dest


def test_snowflake_debug(snowflake_project: Path, snowflake_profiles_dir: Path) -> None:
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()
    result = runner.invoke(
        [
            "debug",
            "--project-dir",
            str(snowflake_project),
            "--profiles-dir",
            str(snowflake_profiles_dir),
        ]
    )
    assert result.success, result.exception


def test_snowflake_run(snowflake_project: Path, snowflake_profiles_dir: Path) -> None:
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()
    result = runner.invoke(
        [
            "run",
            "--project-dir",
            str(snowflake_project),
            "--profiles-dir",
            str(snowflake_profiles_dir),
        ]
    )
    assert result.success, f"dbt run failed: {result.exception}\n" + "\n".join(
        str(r) for r in (result.result or [])
    )
