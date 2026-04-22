# dbt-burla

A dbt adapter that runs **Python models** on a [Burla](https://burla.dev)
cluster while your SQL models keep running in DuckDB, Snowflake, or BigQuery.

## Why

dbt's support for Python models today is gated behind Snowpark, Dataproc, or
Databricks - meaning if you want to drop a Python model into your dbt DAG you
have to stand up (and pay for) a Spark cluster even when the work is
embarrassingly parallel per-row: ML scoring, API calls, LLM enrichment,
feature engineering, simulation.

`dbt-burla` gives you a third option: let dbt's DAG, testing, lineage, and
documentation stay intact, and let Burla's elastic compute run the Python
without Spark.

## Contents

- [Quickstart](quickstart.md) - get a working pipeline in under 5 minutes
- [Configuration reference](configuration.md) - every `burla_*` knob
- [How it works](how-it-works.md) - end-to-end walkthrough of a Python model run
- Per-warehouse setup:
  - [DuckDB](warehouses/duckdb.md)
  - [Snowflake](warehouses/snowflake.md)
  - [BigQuery](warehouses/bigquery.md)
- [Troubleshooting](troubleshooting.md)

## Compatibility

| dbt-core | Python              | DuckDB | Snowflake | BigQuery |
| -------- | ------------------- | ------ | --------- | -------- |
| 1.8.x    | 3.11 / 3.12 / 3.13  | ✓      | ✓         | ✓        |
| 1.9.x    | 3.11 / 3.12 / 3.13  | ✓      | ✓         | ✓        |

dbt Fusion (Rust) is planned but not yet supported.

## When *not* to use this

- Your dbt project is 100% SQL - you're not going to get anything
  interesting out of `dbt-burla`. Use vanilla `dbt-snowflake` / `dbt-bigquery`.
- Your Python work is trivial per-row - adding the pandas<->warehouse
  round-trip is slower than the transform itself.
- You're on a warehouse we don't yet support (Postgres, Redshift, etc.) -
  file an issue, most of the scaffolding is warehouse-agnostic.
