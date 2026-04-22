# Quickstart: DuckDB + Burla (zero setup)

The fastest way to see `dbt-burla` in action. No cloud, no credentials, no
Spark. Runs in seconds.

## Setup

```bash
pip install "dbt-burla[duckdb]"
```

## Run it

```bash
cd examples/01-quickstart-duckdb
dbt run --profiles-dir .
```

You should see three models run:

- `stg_orders.sql` - a plain SQL view over some inline data
- `enriched_orders.py` - a Python model that uses pandas to add
  computed columns
- `order_summary.sql` - a downstream SQL model aggregating the
  Python model's output

Open `warehouse.duckdb` with any DuckDB client to inspect the tables:

```bash
duckdb warehouse.duckdb
> select * from main.enriched_orders;
```

## What just happened

Your `profiles.yml` selects the `burla_duckdb` adapter with `burla_fake: true`.
That tells `dbt-burla` to run Python models in-process (no cluster needed),
while still exercising the full code path that would be used on a real cluster.

Flip `burla_fake: false` and supply `burla_cluster_url` to run the same Python
model on 1,000 Burla workers instead.

## Try it next

- Add another Python model that calls `burla.remote_parallel_map` for
  per-row work (see [`04-fan-out-heavy-compute`](../04-fan-out-heavy-compute/))
- Swap in your own warehouse: change `type: burla_duckdb` to
  `burla_snowflake` or `burla_bigquery` (see the [Snowflake](../02-snowflake-ml-scoring/)
  or [BigQuery](../03-bigquery-llm-enrichment/) examples).
