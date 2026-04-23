# DuckDB setup

> **Use this warehouse when** you want zero-setup local development, CI integration tests, or a self-contained demo. DuckDB is embedded, ephemeral, and single-machine - perfect for everything except production pipelines.

`burla_duckdb` wraps [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb)
and routes Python models through Burla.

## Install

```bash
pip install "dbt-burla[duckdb]"
```

This pulls in `dbt-duckdb`, `duckdb`, and the Burla client.

## `profiles.yml`

Start from your existing `dbt-duckdb` profile and swap `type: duckdb`
for `type: burla_duckdb`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: burla_duckdb
      path: ./warehouse.duckdb
      schema: main
      threads: 1
      burla_default_workers: 16
      # burla_cluster_url: https://cluster.burla.dev
      # burla_fake: true           # recommended for local dev / CI
```

Everything `dbt-duckdb` supports works: `path: ":memory:"`, `attach:`,
`filesystems:`, `extensions:`, etc.

## When this is the right pick

- **Local development**, especially if you iterate on Python models
  without wanting to hit a real cluster. Set `burla_fake: true`.
- **CI**. DuckDB is embedded, zero setup, and the full Python model code
  path still runs.
- **Demos and tutorials**. See the [quickstart example](https://github.com/Burla-Cloud/dbt-burla/tree/main/examples/01-quickstart-duckdb).

## When *not* to use it

- **Production**. DuckDB is single-machine and ephemeral. For production
  data pipelines use `burla_snowflake` or `burla_bigquery`.

## Known caveats

- `view` materialization for Python is not supported (dbt doesn't compile
  Python as views - this is a dbt-core limitation).
- DuckDB connections are per-thread inside dbt; `dbt-burla` reuses the
  thread connection already open for SQL.
