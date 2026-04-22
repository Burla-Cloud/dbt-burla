<p align="center">
  <a href="https://burla.dev">
    <img src="https://backend.burla.dev/static/logo.svg" width="240">
  </a>
</p>

<h1 align="center">dbt-burla</h1>

<p align="center">
  <em>Run dbt Python models on a <a href="https://burla.dev">Burla</a> cluster - no Spark, no Snowpark, no Dataproc.</em>
</p>

<p align="center">
  <a href="https://pypi.org/project/dbt-burla/"><img src="https://img.shields.io/pypi/v/dbt-burla?style=for-the-badge" height="22"></a>
  <a href="https://github.com/Burla-Cloud/dbt-burla/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Burla-Cloud/dbt-burla/ci.yml?style=for-the-badge" height="22"></a>
  <a href="https://github.com/Burla-Cloud/dbt-burla/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-FSL--1.1--Apache--2.0-blue?style=for-the-badge" height="22"></a>
</p>

---

## What this is

`dbt-burla` is a [dbt](https://getdbt.com) adapter that wraps your existing warehouse (DuckDB, Snowflake, or BigQuery) and routes **Python models** to a [Burla](https://burla.dev) cluster instead of Snowpark / Dataproc / Databricks.

- Your SQL models keep running in your warehouse, unchanged.
- Your Python models run on elastic Burla compute - scale to 1,000+ VMs in one call, any Docker image, any `func_cpu`/`func_ram`.
- Lineage, tests, docs, incremental models - everything dbt already gives you - stays intact.

## Install

```bash
pip install "dbt-burla[duckdb]"        # self-contained, zero-setup
pip install "dbt-burla[snowflake]"     # production target
pip install "dbt-burla[bigquery]"      # Google Cloud target
pip install "dbt-burla[all]"           # all warehouses
```

## 60-second quickstart (DuckDB)

```bash
git clone https://github.com/Burla-Cloud/dbt-burla.git
cd dbt-burla/examples/01-quickstart-duckdb
pip install "dbt-burla[duckdb]"
dbt run --profiles-dir .
```

You'll see three SQL models run in DuckDB, then a Python model execute in-process (because `burla_fake: true` is set). Flip that to `false` and set `burla_cluster_url` to run the Python model on a real cluster.

## How it works

```text
        dbt CLI
           │
           ▼
  ┌──────────────────┐
  │  BurlaAdapter    │
  │                  │
  │  SQL models ─────┼──► [warehouse]
  │                  │
  │  Python models   │
  │    1. read ──────┼──► [warehouse]
  │    2. submit ────┼──► [Burla cluster]
  │    3. write ─────┼──► [warehouse]
  └──────────────────┘
```

See [docs/how-it-works.md](docs/how-it-works.md) for a deeper walkthrough.

## Writing a Python model

```python
import pandas as pd
from burla import remote_parallel_map


def score_one(row: dict) -> dict:
    row["score"] = (row["amount"] or 0) * 0.01
    return row


def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=100,
        burla_cpus_per_worker=4,
    )
    orders = dbt.ref("stg_orders")
    scored = list(remote_parallel_map(score_one, orders.to_dict("records")))
    return pd.DataFrame(scored)
```

Configure it once in `profiles.yml` - use the adapter type that matches your warehouse:

```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: burla_snowflake     # or burla_duckdb / burla_bigquery
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      schema: analytics
      # Burla-specific defaults (all optional)
      burla_default_workers: 100
      burla_default_cpus_per_worker: 4
      # burla_cluster_url: https://cluster.burla.dev
      # burla_fake: true       # run Python models in-process (useful for local / CI)
```

Every knob your warehouse's own `dbt-*` adapter supports still works - `dbt-burla` is a subclass of the matching warehouse adapter.

## Compatibility

| dbt-core | Python       | DuckDB | Snowflake | BigQuery |
| -------- | ------------ | ------ | --------- | -------- |
| 1.8.x    | 3.11 / 3.12 / 3.13 | ✓ | ✓ | ✓ |
| 1.9.x    | 3.11 / 3.12 / 3.13 | ✓ | ✓ | ✓ |

dbt Fusion (Rust) is tracked for a future release.

## Documentation

- [Quickstart](docs/quickstart.md)
- [Configuration reference](docs/configuration.md)
- [How it works](docs/how-it-works.md)
- Per-warehouse guides: [DuckDB](docs/warehouses/duckdb.md) · [Snowflake](docs/warehouses/snowflake.md) · [BigQuery](docs/warehouses/bigquery.md)
- [Troubleshooting](docs/troubleshooting.md)

## Examples

- [`01-quickstart-duckdb`](examples/01-quickstart-duckdb/) - zero-setup, runs on DuckDB + fake Burla
- [`02-snowflake-ml-scoring`](examples/02-snowflake-ml-scoring/) - per-row ML inference, fanned out on Burla
- [`03-bigquery-llm-enrichment`](examples/03-bigquery-llm-enrichment/) - per-row LLM API calls
- [`04-fan-out-heavy-compute`](examples/04-fan-out-heavy-compute/) - massive `remote_parallel_map` inside a dbt model

## Status

`v0.x` - API may change before `v1.0`. Follow the [changelog](CHANGELOG.md) for breaking changes.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Short version: `make install && make test`.

## License

[FSL-1.1-Apache-2.0](LICENSE). Source-available today, Apache 2.0 in two years.
