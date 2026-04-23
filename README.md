<p align="center">
  <a href="https://burla.dev">
    <img src="https://backend.burla.dev/static/logo.svg" width="240">
  </a>
</p>

<h1 align="center">dbt-burla</h1>

<p align="center">
  <strong>Run dbt Python models on 1,000 CPUs in 1 second.</strong><br>
  <em>No Snowpark. No Dataproc. No Databricks.</em>
</p>

<p align="center">
  <a href="https://pypi.org/project/dbt-burla/"><img src="https://img.shields.io/pypi/v/dbt-burla?style=for-the-badge" height="22"></a>
  <a href="https://github.com/Burla-Cloud/dbt-burla/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Burla-Cloud/dbt-burla/ci.yml?style=for-the-badge" height="22"></a>
  <a href="https://burla-cloud.github.io/dbt-burla/"><img src="https://img.shields.io/badge/docs-gitbook-3C5B65?style=for-the-badge&logo=gitbook&logoColor=white" height="22"></a>
  <a href="https://github.com/Burla-Cloud/dbt-burla/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-FSL--1.1--Apache--2.0-blue?style=for-the-badge" height="22"></a>
</p>

---

## The problem

**dbt's Python model support is gated behind Spark.**

If you want to run Python in a dbt DAG today, you have three options:

1. **Snowpark** - Python runs as a Snowflake stored procedure. Expensive compute. No custom images. No GPUs.
2. **Dataproc** - Python runs on a managed Google Cloud Spark cluster. Minutes of startup time per model.
3. **Databricks** - Python runs on a Databricks cluster you provisioned and maintain. Separate bill, separate platform.

All three assume you want **Spark** - JVM-based distributed DataFrames, designed for terabyte-scale distributed joins.

But most Python work in a dbt DAG *isn't* that. It's:

- ML scoring: run `model.predict(row)` on every row
- LLM enrichment: call an API for each row
- Feature engineering: compute 20 new columns per row
- Per-row simulation, classification, embedding, OCR, geocoding, NLP

**This work is embarrassingly parallel.** You don't need a Spark cluster with a JVM. You need a lot of Python processes running at the same time.

Today that leaves you choosing between:

- Writing awkward SQL for things that should be Python
- Writing Python outside dbt (losing lineage, tests, materializations)
- Paying for Spark compute you don't actually need

## The solution

**`dbt-burla`** is a dbt adapter that runs Python models on a [Burla](https://burla.dev) cluster instead of Snowpark / Dataproc / Databricks.

[Burla](https://burla.dev) is a Python-first compute platform: one function call gives you up to 10,000 parallel VMs in about a second. Any Docker image. Any CPU/RAM shape. Any GPU.

`dbt-burla` plugs that into dbt. Your Python models still look like normal dbt Python models - `def model(dbt, session): ...` - but they run on Burla workers. Your SQL models keep running in your warehouse, unchanged. Lineage, `ref()`, tests, docs, incremental models - all still work.

```python title="models/scored_customers.py"
import pandas as pd
from burla import remote_parallel_map


def score(row: dict) -> dict:
    row["churn_score"] = my_model.predict(row)
    return row


def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=500,           # 500 VMs, each runs score() on a batch
        burla_cpus_per_worker=4,
    )
    customers = dbt.ref("stg_customers").to_dict("records")
    scored = list(remote_parallel_map(score, customers))
    return pd.DataFrame(scored)
```

```yaml title="profiles.yml"
my_project:
  target: prod
  outputs:
    prod:
      type: burla_snowflake          # or burla_duckdb / burla_bigquery
      # ... your normal Snowflake config ...
      burla_default_workers: 100
      burla_default_cpus_per_worker: 4
```

That's it. No Spark cluster to set up. No compute to pre-provision. Results land back in Snowflake; downstream SQL models `ref()` the table like any other.

## Why you'd actually use this

**You're already using dbt and want to add real Python work to your DAG.** Not SQL-dressed-up-as-Python, but Python that hits APIs, runs ML models, makes LLM calls, does heavy per-row computation. You want to keep dbt's lineage, testing, and documentation story intact.

**You've hit the wall of Snowpark's Python support.** Complex named UDFs don't work. Package installation is painful. Cold starts are slow. Compute cost scales with warehouse size, which doesn't map well to per-row Python work.

**You don't want to run and pay for a Spark cluster.** Databricks and Dataproc are powerful but heavyweight. You don't need distributed shuffles; you need 500 workers doing per-row Python for 30 seconds. Spark's fixed cluster model is wasteful for this.

**You want to iterate locally.** `burla_fake: true` runs every Python model in-process, so your entire dbt project runs on your laptop with no cluster, no cloud, no credentials. Tests run in CI the same way.

**Concrete workloads that fit perfectly:**

- Classifying / embedding / summarizing text rows with an LLM
- Scoring rows through a PyTorch or XGBoost model cached from GCS
- Calling third-party enrichment APIs per row (stripe, clearbit, openai, openweather)
- Running geospatial, imaging, or scientific-Python work on rows
- Heavy per-row feature engineering that's gross in SQL

## When `dbt-burla` is NOT the right pick

Be honest with yourself:

- **Your project is 100% SQL.** You don't need this. Stick with `dbt-duckdb` / `dbt-snowflake` / `dbt-bigquery`.
- **You're doing distributed joins or shuffles at terabyte scale.** That's Spark's job, not Burla's. Use `dbt-snowflake` + Snowpark, or `dbt-databricks`.
- **Your warehouse isn't supported.** We ship DuckDB, Snowflake, and BigQuery today. Postgres, Redshift, Fabric, etc. aren't supported yet. File an issue - most of the scaffolding is warehouse-agnostic.
- **You can't pull upstream tables through pandas.** For very large upstream inputs (hundreds of millions of rows) you should aggregate in SQL first so the Python model gets a manageable input.

## `dbt-burla` vs the alternatives

|                                              | `dbt-burla` | Snowpark (`dbt-snowflake`) | Dataproc (`dbt-bigquery`) | Databricks (`dbt-databricks`) |
| :------------------------------------------- | :---------: | :------------------------: | :-----------------------: | :---------------------------: |
| No Spark / JVM cluster to manage             |      ✓      |             ✓              |                           |                               |
| Sub-second worker startup                    |      ✓      |                            |                           |                               |
| Scales to 1,000+ VMs in one call             |      ✓      |                            |                           |                               |
| Per-model hardware config (CPUs / RAM / GPU) |      ✓      |                            |             ✓             |               ✓               |
| Any Docker image for your workers            |      ✓      |                            |             ✓             |               ✓               |
| Runs in local dev with no cloud              |      ✓      |                            |                           |                               |
| Works with DuckDB                            |      ✓      |                            |                           |                               |
| Works with Snowflake                         |      ✓      |             ✓              |                           |                               |
| Works with BigQuery                          |      ✓      |                            |             ✓             |                               |
| Distributed joins / shuffles                 |             |             ✓              |             ✓             |               ✓               |
| Snowflake-native Python UDFs                 |             |             ✓              |                           |                               |

---

## Install

```bash
pip install "dbt-burla[duckdb]"        # zero-setup, local / CI
pip install "dbt-burla[snowflake]"     # production on Snowflake
pip install "dbt-burla[bigquery]"      # production on BigQuery
pip install "dbt-burla[all]"           # all three
```

## 60-second quickstart

```bash
git clone https://github.com/Burla-Cloud/dbt-burla.git
cd dbt-burla/examples/01-quickstart-duckdb
pip install "dbt-burla[duckdb]"
dbt run --profiles-dir .
```

Three models run end-to-end in DuckDB: a SQL view, a Python `table` model, and a downstream SQL aggregate. No cloud, no cluster, no credentials. Flip `burla_fake: false` and the Python model runs on a real Burla cluster instead.

**[Full quickstart →](https://burla-cloud.github.io/dbt-burla/quickstart/)**

## How it works

```text
               dbt CLI
                  │
                  ▼
          ┌───────────────────────┐
          │    BurlaXxxAdapter    │
          │  (subclasses          │
          │   dbt-duckdb /        │
          │   dbt-snowflake /     │
          │   dbt-bigquery)       │
          └───┬──────────────┬────┘
              │              │
   SQL models │              │ Python models
              │              │
              ▼              ▼
       ┌──────────┐   ┌────────────────────────────┐
       │ warehouse│   │ BurlaPythonJobHelper       │
       │ (runs    │   │                            │
       │  SQL     │   │  1. read upstream refs ──► │──► warehouse
       │  as      │   │  2. submit to Burla    ──► │──► Burla cluster
       │  usual)  │   │  3. write result       ──► │──► warehouse
       └──────────┘   └────────────────────────────┘
```

**[Deep dive into how it works →](https://burla-cloud.github.io/dbt-burla/how-it-works/)**

## Compatibility

| dbt-core | Python              | DuckDB | Snowflake | BigQuery |
| -------- | ------------------- | :----: | :-------: | :------: |
| 1.8.x    | 3.11 / 3.12 / 3.13  | ✓      | ✓         | ✓        |
| 1.9.x    | 3.11 / 3.12 / 3.13  | ✓      | ✓         | ✓        |

dbt Fusion (Rust) is tracked for a future release.

## Documentation

**[burla-cloud.github.io/dbt-burla](https://burla-cloud.github.io/dbt-burla/)**

- **[Quickstart](https://burla-cloud.github.io/dbt-burla/quickstart/)** - get a working pipeline in under 5 minutes
- **[How it works](https://burla-cloud.github.io/dbt-burla/how-it-works/)** - end-to-end walkthrough of a Python model run
- **[Configuration](https://burla-cloud.github.io/dbt-burla/configuration/)** - every `burla_*` knob, at profile and model level
- Per-warehouse setup: **[DuckDB](https://burla-cloud.github.io/dbt-burla/warehouses/duckdb/)** · **[Snowflake](https://burla-cloud.github.io/dbt-burla/warehouses/snowflake/)** · **[BigQuery](https://burla-cloud.github.io/dbt-burla/warehouses/bigquery/)**
- **[Troubleshooting](https://burla-cloud.github.io/dbt-burla/troubleshooting/)**

## Examples

| Example | Warehouse | What it shows |
| ------- | --------- | ------------- |
| [`01-quickstart-duckdb`](examples/01-quickstart-duckdb/) | DuckDB | Zero-setup run; no cluster needed |
| [`02-snowflake-ml-scoring`](examples/02-snowflake-ml-scoring/) | Snowflake | Per-row ML inference fanned out on Burla |
| [`03-bigquery-llm-enrichment`](examples/03-bigquery-llm-enrichment/) | BigQuery | LLM API call per row across a cluster |
| [`04-fan-out-heavy-compute`](examples/04-fan-out-heavy-compute/) | DuckDB | `remote_parallel_map` *inside* a model for massive fan-out |

## Status

`v0.x` - API may change before `v1.0`. Track breaking changes in the [CHANGELOG](CHANGELOG.md). For production use, pin an exact version.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Short version: `make install && make test`.

## License

[FSL-1.1-Apache-2.0](LICENSE). Source-available today, Apache 2.0 in two years.

---

<p align="center">
  Questions? <a href="https://github.com/Burla-Cloud/dbt-burla/issues">Open an issue</a> or <a href="https://cal.com/jakez/burla">schedule a call</a>.
</p>
