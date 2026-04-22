# Contributing to dbt-burla

Thanks for your interest in contributing. This project follows a few standard conventions.

## Development setup

You'll need [`uv`](https://docs.astral.sh/uv/) installed.

```bash
git clone https://github.com/Burla-Cloud/dbt-burla.git
cd dbt-burla
uv sync --all-extras
uv run pre-commit install
```

## Running checks

```bash
make lint         # ruff check
make format       # ruff format + --fix
make typecheck    # mypy (strict)
make test         # pytest -m "unit or duckdb"
make cov          # tests + coverage report (must be >=85%)
```

CI runs all of these plus the full DuckDB integration suite on Python 3.11 / 3.12 / 3.13.

## Running integration tests

- **DuckDB** runs without credentials and is part of `make test`.
- **Snowflake** runs when these env vars are set:
  ```bash
  export SNOWFLAKE_ACCOUNT=...
  export SNOWFLAKE_USER=...
  export SNOWFLAKE_PASSWORD=...
  export SNOWFLAKE_DATABASE=...
  export SNOWFLAKE_WAREHOUSE=...
  export SNOWFLAKE_ROLE=...
  uv run pytest -m snowflake
  ```
- **BigQuery** runs when `GOOGLE_APPLICATION_CREDENTIALS` and `BIGQUERY_PROJECT` are set:
  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
  export BIGQUERY_PROJECT=my-gcp-project
  uv run pytest -m bigquery
  ```
- **Real Burla cluster** smoke test runs when `BURLA_CLUSTER_URL` is set. By default tests use the in-process `fake_burla` executor.

## Making a change

1. Fork and branch from `main`.
2. Keep commits small and focused.
3. Update `CHANGELOG.md` under the `[Unreleased]` section.
4. Open a PR. CI must be green.
5. Squash-merge.

## Releasing

Releases are automated via GitHub Releases:

1. Bump the version in `src/dbt/adapters/burla/__version__.py`.
2. Move the `[Unreleased]` section to a new version in `CHANGELOG.md`.
3. Tag and create a GitHub Release. `release.yml` publishes to PyPI via trusted publishing.

## Code style

- `ruff` for lint + format, `mypy --strict` for types.
- Don't add comments that just narrate the code. Comments explain *why*, not *what*.
- New features need a test. Bug fixes need a regression test.
