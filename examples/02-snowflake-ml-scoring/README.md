# Snowflake + Burla: per-row ML scoring

Shows the pattern that usually drives teams to Snowpark or Dataproc: run an
ML model across every row of a large Snowflake table. With `dbt-burla` you
get a simple `remote_parallel_map` call inside a normal dbt Python model -
no cluster to manage.

## Setup

```bash
pip install "dbt-burla[snowflake]"
```

Copy the profile template and fill in your Snowflake credentials:

```bash
cp profiles.example.yml profiles.yml
$EDITOR profiles.yml
```

Make sure `burla login` is configured (or set `burla_fake: true` in
`profiles.yml` if you just want to see it run locally).

## Run it

```bash
dbt run --profiles-dir .
```

## What it does

`scored_customers.py` reads every row of the `stg_customers` model, fans the
rows out to Burla workers via `remote_parallel_map`, and each worker calls
`score(...)` on one row (your model could be a scikit-learn pickle loaded
from GCS, an XGBoost inference call, an embedding model, etc). The result
DataFrame is written straight back to Snowflake.

The key bits of the model:

```python
from burla import remote_parallel_map

def score(row):
    # Replace with your real model call
    return {**row, "score": row["order_count"] * 0.1}

def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=500,        # 500 VMs in parallel
        burla_cpus_per_worker=2,
        # Use any Docker image with your model + deps baked in:
        # burla_image="us-docker.pkg.dev/my-proj/my-model:latest",
    )
    customers = dbt.ref("stg_customers").to_dict("records")
    scored = list(remote_parallel_map(score, customers))
    return pd.DataFrame(scored)
```

## Tuning

- `burla_workers` - max parallel VMs. Higher = faster for embarrassingly
  parallel work, but costs more per-second.
- `burla_cpus_per_worker` / `burla_ram_per_worker` - shape of each VM.
- `burla_image` - container image to run in. Defaults to Burla's generic
  Python image; override when you need specific C extensions.
