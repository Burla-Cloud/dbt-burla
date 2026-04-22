# Changelog

All notable changes to `dbt-burla` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial release: wrapping adapter for DuckDB, Snowflake, and BigQuery.
- `BurlaPythonJobHelper` that submits dbt Python models to a Burla cluster.
- Warehouse backends with pluggable `read_as_dataframe` / `write_from_dataframe` / `drop_if_exists`.
- `fake_burla` in-process executor for zero-setup testing and the quickstart example.
- Four runnable example projects.
- Full unit + DuckDB integration test suite in CI; Snowflake + BigQuery integration tests gated behind credentials.
- PyPI release automation via GitHub Actions trusted publishing.

[Unreleased]: https://github.com/Burla-Cloud/dbt-burla/compare/v0.1.0...HEAD
