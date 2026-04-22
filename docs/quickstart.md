# Quickstart

The shortest path from nothing to "I saw a dbt Python model run on Burla".

## Option A: zero-setup (DuckDB, fake Burla)

No cluster, no cloud, just pip + dbt run.

```bash
git clone https://github.com/Burla-Cloud/dbt-burla.git
cd dbt-burla/examples/01-quickstart-duckdb
pip install "dbt-burla[duckdb]"
dbt run --profiles-dir .
```

That's it. You now have three models in `warehouse.duckdb`:

- `stg_orders` (SQL view)
- `enriched_orders` (Python model - runs in-process under the hood because `burla_fake: true`)
- `order_summary` (downstream SQL)

Open the warehouse:

```bash
duckdb warehouse.duckdb
> select * from main.enriched_orders;
```

## Option B: real Burla cluster + your warehouse

Once you're past the quickstart, drop `dbt-burla` into a real project.

### 1. Install

```bash
pip install "dbt-burla[snowflake]"   # or [bigquery] / [duckdb]
```

### 2. Configure Burla

If you haven't already:

```bash
burla login
```

### 3. Point `profiles.yml` at `dbt-burla`

Every field your existing `dbt-snowflake` / `dbt-bigquery` / `dbt-duckdb`
profile uses still works - just change `type:` to the Burla variant and add
the `burla_*` knobs you want.

```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: burla_snowflake   # was: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: PROD_DB
      warehouse: PROD_WH
      schema: analytics
      threads: 4

      # Burla-specific
      burla_cluster_url: https://my-cluster.burla.dev
      burla_default_workers: 100
      burla_default_cpus_per_worker: 4
```

### 4. Write a Python model

```python
# models/enriched_orders.py
import pandas as pd
from burla import remote_parallel_map


def enrich(row: dict) -> dict:
    row["amount_cents"] = int((row["amount"] or 0) * 100)
    return row


def model(dbt, session):
    dbt.config(materialized="table")
    orders = dbt.ref("stg_orders")
    enriched = list(remote_parallel_map(enrich, orders.to_dict("records")))
    return pd.DataFrame(enriched)
```

### 5. Run it

```bash
dbt run --select enriched_orders
```

Watch Burla spin up workers and run your model. The result lands as a normal
table in Snowflake, which downstream SQL models can `ref()` like any other.

## Next steps

- [Configuration reference](configuration.md) - fine-tune Burla behavior
  per-model and per-profile
- [How it works](how-it-works.md) - understand the data flow end-to-end
- [`examples/`](../examples/) - four runnable reference projects
