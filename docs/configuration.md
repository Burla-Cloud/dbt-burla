# Configuration reference

Every `burla_*` knob you can set, at both the **profile** level
(`profiles.yml`) and the **model** level (`dbt.config(...)` or
`schema.yml`). Model-level config wins.

## Profile-level (defaults for every Python model)

Set these inside your `outputs.<target>` block in `profiles.yml`.

| Key                            | Type  | Default | Purpose |
| ------------------------------ | ----- | ------- | ------- |
| `burla_cluster_url`            | str   | *uses `burla login` credentials* | Override the cluster URL. Sets `BURLA_CLUSTER_DASHBOARD_URL` before each submission. |
| `burla_default_workers`        | int   | `16`    | Max concurrent Burla workers per model (`func_cpu` on the client). |
| `burla_default_cpus_per_worker`| int   | `1`     | CPUs per worker VM. |
| `burla_default_ram_per_worker` | int   | `None`  | Optional RAM (GB) per worker VM. |
| `burla_default_image`          | str   | `None`  | Custom Docker image for workers (`us-docker.pkg.dev/.../img:tag`). |
| `burla_default_timeout_s`      | int   | `3600`  | Per-model timeout in seconds. |
| `burla_fake`                   | bool  | `false` | Run Python models in-process instead of shipping to Burla. Great for CI and local development. When true, any `burla.remote_parallel_map` calls inside your model also run in-process. |

All other profile fields your warehouse adapter supports
(`dbt-snowflake`, `dbt-bigquery`, `dbt-duckdb`) still work unchanged -
`dbt-burla` is a subclass.

### Example (Snowflake)

```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: burla_snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: PROD
      warehouse: PROD_WH
      schema: analytics
      threads: 8
      # Burla:
      burla_cluster_url: https://cluster.burla.dev
      burla_default_workers: 200
      burla_default_cpus_per_worker: 4
      burla_default_image: us-docker.pkg.dev/my-proj/worker:latest
```

## Model-level (per-model overrides)

Set these inside the Python model via `dbt.config(...)` or in a yaml model
config block.

| Key                   | Type | Default | Purpose |
| --------------------- | ---- | ------- | ------- |
| `burla_workers`        | int  | from profile | Override concurrent workers for this model. |
| `burla_cpus_per_worker`| int  | from profile | CPUs per worker for this model. |
| `burla_ram_per_worker` | int  | from profile | RAM (GB) per worker for this model. |
| `burla_image`          | str  | from profile | Override worker image for this model. |
| `burla_timeout_s`      | int  | from profile | Per-model timeout override. |
| `burla_fake`           | bool | from profile | Override fake-mode for this model. |

### Example

```python
def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=500,
        burla_cpus_per_worker=2,
        burla_image="us-docker.pkg.dev/my-proj/heavy-worker:latest",
    )
    ...
```

Or via YAML:

```yaml
version: 2
models:
  - name: scored_customers
    config:
      materialized: table
      burla_workers: 500
      burla_cpus_per_worker: 2
```

## Supported materializations

| Materialization | Python | SQL | Notes |
| --------------- | :----: | :-: | ----- |
| `table`         | ✓      | ✓   | Writes replace the target table atomically. |
| `incremental`   | ✓      | ✓   | Python models must return a DataFrame of *new* rows; `dbt-burla` appends. Uses `append` write mode. |
| `view`          | -      | ✓   | Not meaningful for Python (dbt doesn't compile Python as a view). |

## Environment variables used

- `BURLA_CLUSTER_DASHBOARD_URL` - set by the adapter from `burla_cluster_url`
  before each submission. You can also set it directly in the environment
  and leave `burla_cluster_url` unset.
- Standard `dbt`/warehouse env vars (`SNOWFLAKE_*`, `BIGQUERY_PROJECT`, etc.)
  are honored because `dbt-burla` reuses your warehouse adapter's config.
