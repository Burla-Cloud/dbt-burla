"""Integration tests for `burla_bigquery`. Skipped unless BigQuery creds are present."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.bigquery


@pytest.fixture
def bigquery_profiles_dir(tmp_path: Path, bigquery_env: dict[str, str]) -> Path:
    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    dataset = os.environ.get("BIGQUERY_DATASET", f"dbt_burla_ci_{os.getpid()}")
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    method = "service-account" if keyfile else "oauth"
    keyfile_line = f"\n      keyfile: {keyfile}" if keyfile else ""
    (profile_dir / "profiles.yml").write_text(
        f"""
burla_bigquery_basic:
  target: dev
  outputs:
    dev:
      type: burla_bigquery
      method: {method}{keyfile_line}
      project: {bigquery_env["BIGQUERY_PROJECT"]}
      dataset: {dataset}
      threads: 1
      burla_fake: true
""".strip()
    )
    return profile_dir


@pytest.fixture
def bigquery_project(tmp_path: Path) -> Path:
    import shutil

    src = Path(__file__).parent / "projects" / "duckdb_basic"
    dest = tmp_path / "project"
    shutil.copytree(src, dest)
    proj_yml = dest / "dbt_project.yml"
    proj_yml.write_text(proj_yml.read_text().replace("burla_duckdb_basic", "burla_bigquery_basic"))
    return dest


def test_bigquery_debug(bigquery_project: Path, bigquery_profiles_dir: Path) -> None:
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()
    result = runner.invoke(
        [
            "debug",
            "--project-dir",
            str(bigquery_project),
            "--profiles-dir",
            str(bigquery_profiles_dir),
        ]
    )
    assert result.success, result.exception


def test_bigquery_run(bigquery_project: Path, bigquery_profiles_dir: Path) -> None:
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()
    result = runner.invoke(
        [
            "run",
            "--project-dir",
            str(bigquery_project),
            "--profiles-dir",
            str(bigquery_profiles_dir),
        ]
    )
    assert result.success, f"dbt run failed: {result.exception}\n" + "\n".join(
        str(r) for r in (result.result or [])
    )
