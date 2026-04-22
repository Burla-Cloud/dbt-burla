# Fan-out heavy compute inside a Python model

This example is about the "embarrassingly parallel per-row work" pattern:
your model isn't just one SQL-ish transformation, it runs real Python work
per row and benefits from running on many VMs at once.

## Setup

```bash
pip install "dbt-burla[duckdb]"
```

## Run it

```bash
cd examples/04-fan-out-heavy-compute
dbt run --profiles-dir .
```

Like the quickstart, this example defaults to `burla_fake: true` so it runs
without a cluster. Set `burla_fake: false` + `burla_cluster_url` to run on
a real Burla cluster and watch the fan-out.

## What it does

`computed_features.py` simulates CPU-heavy per-row work (a Monte-Carlo
estimate of pi, scaled by each row's value - any similar per-row
computation would fit the pattern). Each row is sent to a Burla worker;
the results come back and are written to DuckDB.

```python
from burla import remote_parallel_map

def heavy_work(row: dict) -> dict:
    # CPU-heavy per-row computation
    ...
    return {**row, "feature_a": ..., "feature_b": ...}

def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=1000,       # 1000 workers
        burla_cpus_per_worker=4,
    )
    rows = dbt.ref("stg_inputs").to_dict("records")
    enriched = list(remote_parallel_map(heavy_work, rows))
    return pd.DataFrame(enriched)
```

## When to use this pattern

- Per-row ML inference
- Per-row API calls (translation, enrichment, scraping, LLM)
- Heavy numerical work (simulation, optimization)
- Any workload where the per-row function takes longer than a few ms

For very short per-row work, you want to batch rows before submitting - see
the [configuration docs](../../docs/configuration.md) for `burla_batch_size`.
