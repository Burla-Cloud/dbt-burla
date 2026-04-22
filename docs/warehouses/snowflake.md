# Snowflake setup

`burla_snowflake` wraps [`dbt-snowflake`](https://github.com/dbt-labs/dbt-snowflake)
and routes Python models to Burla instead of Snowpark.

## Install

```bash
pip install "dbt-burla[snowflake]"
```

This pulls in `dbt-snowflake`, `snowflake-connector-python[pandas]`, and
the Burla client.

## `profiles.yml`

Start from your existing `dbt-snowflake` profile and swap `type: snowflake`
for `type: burla_snowflake`:

```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: burla_snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'SYSADMIN') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: analytics
      threads: 8

      # Burla-specific:
      burla_cluster_url: https://cluster.burla.dev
      burla_default_workers: 200
      burla_default_cpus_per_worker: 4
```

All of `dbt-snowflake`'s auth flavors work (password, OAuth, key pair,
externalbrowser, etc.) - `dbt-burla` inherits the full credentials class.

## Data movement

For Python models, upstream tables are pulled into pandas on the dbt-running
machine via `snowflake-connector-python`'s `fetch_pandas_all()`. Results are
uploaded back via `write_pandas` through a staging table for atomic swap.

For very large upstream tables (hundreds of millions of rows), either:

1. Aggregate or sample in SQL first so the Python model gets a manageable input, or
2. Use Burla's mounted NFS to hand off parquet files directly (coming in a future release).

## Running SQL models

SQL models continue to run in Snowflake exactly as they would under vanilla
`dbt-snowflake`. No difference in behavior.

## Costs

Two things cost money here:

1. **Snowflake warehouse time** to read/write the DataFrames.
2. **Burla worker time** to run the Python model.

Set `burla_workers` thoughtfully for ML / API-bound workloads: too high
and you can hit rate limits or waste spin-up cost; too low and you're
leaving parallelism on the table.

## Known caveats

- Iceberg table format is not yet supported for Python models (mirrors
  `dbt-snowflake`'s own limitation).
- We use `write_pandas` with `auto_create_table=True` - the schema of the
  DataFrame determines the target table schema. Type mismatches on
  subsequent runs (e.g., a column widens from int to float) may require
  a `--full-refresh`.
