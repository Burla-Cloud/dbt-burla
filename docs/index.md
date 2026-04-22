---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

# Run dbt Python models on 1,000 CPUs in 1 second.

<p class="hero-sub" markdown>
  **dbt-burla** is a dbt adapter that runs Python models on a [Burla](https://burla.dev) cluster instead of Snowpark, Dataproc, or Databricks - while your SQL keeps running in DuckDB, Snowflake, or BigQuery.
</p>

<div class="hero-buttons" markdown>
[Get started in 60 seconds :material-rocket-launch:](quickstart.md){ .md-button .md-button--primary }
[View on GitHub :fontawesome-brands-github:](https://github.com/Burla-Cloud/dbt-burla){ .md-button }
</div>

```python title="models/customer_features.py"
import pandas as pd
from burla import remote_parallel_map


def score(row: dict) -> dict:
    row["score"] = my_model.predict(row)
    return row


def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=500,           # 500 VMs in parallel
        burla_cpus_per_worker=4,
    )
    rows = dbt.ref("stg_customers").to_dict("records")
    scored = list(remote_parallel_map(score, rows))
    return pd.DataFrame(scored)
```

</div>

## Why

dbt's Python model support is gated behind Snowpark, Dataproc, or Databricks - so if you want to drop a Python model into your dbt DAG you have to stand up (and pay for) a Spark cluster, even when the work is embarrassingly parallel per-row: ML scoring, API calls, LLM enrichment, feature engineering, simulation.

**`dbt-burla` gives you a third option**: let dbt's DAG, testing, lineage, and documentation stay intact, and let Burla's elastic compute run the Python without Spark.

<div class="grid cards" markdown>

-   :material-lightning-bolt:{ .lg .middle } **Elastic, per-function hardware**

    ---

    Scale each Python model independently to hundreds or thousands of Burla workers.  
    Set `burla_workers`, `burla_cpus_per_worker`, and `burla_image` per model.

-   :material-database:{ .lg .middle } **Bring your warehouse**

    ---

    First-class support for **DuckDB**, **Snowflake**, and **BigQuery**. SQL models run unchanged; only Python is routed to Burla.

-   :material-check-all:{ .lg .middle } **Everything dbt gives you**

    ---

    Lineage, `ref()`, `source()`, tests, docs, incremental models, `is_incremental`, and snapshots all work exactly as in dbt-core.

-   :material-flash:{ .lg .middle } **Zero-setup local dev**

    ---

    Set `burla_fake: true` to run Python models in-process - no cluster, no cloud, no credentials. Perfect for local dev and CI.

-   :material-docker:{ .lg .middle } **Any Docker image**

    ---

    Set `burla_image` per model to ship with your own dependencies, CUDA, proprietary libraries, or ML models baked in.

-   :material-shield-check:{ .lg .middle } **Production ready**

    ---

    `mypy --strict`, 97% test coverage, CI on Python 3.11/3.12/3.13, dbt-core 1.8 and 1.9, real integration tests against every warehouse.

</div>

## Install

<div class="grid" markdown>

```bash title="DuckDB - zero setup"
pip install "dbt-burla[duckdb]"
```

```bash title="Snowflake"
pip install "dbt-burla[snowflake]"
```

```bash title="BigQuery"
pip install "dbt-burla[bigquery]"
```

</div>

## 60-second quickstart

```bash
git clone https://github.com/Burla-Cloud/dbt-burla.git
cd dbt-burla/examples/01-quickstart-duckdb
pip install "dbt-burla[duckdb]"
dbt run --profiles-dir .
```

Three models run end-to-end: a SQL view, a Python `table` model, and a downstream SQL aggregate. No cloud, no cluster, no credentials. Flip `burla_fake: false` and your Python models run on a real Burla cluster.

[Full quickstart guide :material-arrow-right:](quickstart.md){ .md-button }

## How it works

```mermaid
flowchart LR
    cli[dbt CLI]
    adapter[BurlaXxxAdapter]
    wh[(Warehouse<br/>DuckDB / Snowflake / BigQuery)]
    burla[Burla cluster]

    cli --> adapter
    adapter -- SQL models --> wh
    adapter -- "read upstream" --> wh
    adapter -- "submit model()" --> burla
    burla -- DataFrame --> adapter
    adapter -- "write result" --> wh
```

[Deep dive :material-arrow-right:](how-it-works.md){ .md-button }

## Compatibility

<div class="compat-table" markdown>

| dbt-core | Python              | DuckDB | Snowflake | BigQuery |
| -------- | ------------------- | :----: | :-------: | :------: |
| 1.8.x    | 3.11 / 3.12 / 3.13  | ✓      | ✓         | ✓        |
| 1.9.x    | 3.11 / 3.12 / 3.13  | ✓      | ✓         | ✓        |

</div>

dbt Fusion (Rust) is planned but not yet supported.

## Examples

<div class="grid cards" markdown>

- [:material-rocket-launch-outline: **Quickstart (DuckDB)**](https://github.com/Burla-Cloud/dbt-burla/tree/main/examples/01-quickstart-duckdb)

    Zero-setup, self-contained, runs in seconds.

- [:material-brain: **Snowflake ML scoring**](https://github.com/Burla-Cloud/dbt-burla/tree/main/examples/02-snowflake-ml-scoring)

    Fan out ML inference to 500 workers over a Snowflake table.

- [:material-chat-processing: **BigQuery LLM enrichment**](https://github.com/Burla-Cloud/dbt-burla/tree/main/examples/03-bigquery-llm-enrichment)

    Per-row LLM API calls across a BigQuery dataset.

- [:material-arrow-expand: **Heavy compute fan-out**](https://github.com/Burla-Cloud/dbt-burla/tree/main/examples/04-fan-out-heavy-compute)

    `remote_parallel_map` inside a model for massive parallelism.

</div>

## Status

`v0.x` - API may change before `v1.0`. Follow the [CHANGELOG](https://github.com/Burla-Cloud/dbt-burla/blob/main/CHANGELOG.md) for breaking changes.

Questions? [Open an issue](https://github.com/Burla-Cloud/dbt-burla/issues) or [schedule a call](https://cal.com/jakez/burla).
