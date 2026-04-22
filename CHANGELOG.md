# Changelog

All notable changes to `dbt-burla` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-22

### Added

- Initial release: wrapping adapters for DuckDB (`burla_duckdb`), Snowflake (`burla_snowflake`), and BigQuery (`burla_bigquery`).
- `BurlaPythonJobHelper` that submits dbt Python models to a Burla cluster via `remote_parallel_map`.
- Warehouse backends with pluggable `read_as_dataframe` / `write_from_dataframe` / `drop_if_exists`.
- `burla_fake: true` in-process executor for zero-setup local dev and CI.
- Four runnable example projects (`examples/`).
- Full unit + DuckDB integration test suite in CI; Snowflake + BigQuery integration tests gated behind credentials.
- PyPI release automation via GitHub Actions (trusted publishing + token fallback).
- Documentation site deployed to [burla-cloud.github.io/dbt-burla](https://burla-cloud.github.io/dbt-burla/).

[Unreleased]: https://github.com/Burla-Cloud/dbt-burla/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Burla-Cloud/dbt-burla/releases/tag/v0.1.0
