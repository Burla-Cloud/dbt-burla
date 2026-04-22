# BigQuery setup

`burla_bigquery` wraps [`dbt-bigquery`](https://github.com/dbt-labs/dbt-bigquery)
and routes Python models to Burla instead of Dataproc.

## Install

```bash
pip install "dbt-burla[bigquery]"
```

This pulls in `dbt-bigquery`, `pandas-gbq`, and the Burla client.

## Authentication

Any auth method `dbt-bigquery` supports works - most commonly:

```bash
gcloud auth application-default login
```

Or a service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
```

## `profiles.yml`

Start from your `dbt-bigquery` profile and swap `type: bigquery` for
`type: burla_bigquery`:

```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: burla_bigquery
      method: oauth
      project: "{{ env_var('BIGQUERY_PROJECT') }}"
      dataset: analytics
      location: US
      threads: 8

      # Burla-specific:
      burla_cluster_url: https://cluster.burla.dev
      burla_default_workers: 100
      burla_default_cpus_per_worker: 2
```

## Data movement

For Python models, upstream tables are pulled into pandas via the BigQuery
Storage API (`to_dataframe(create_bqstorage_client=True)`) which is the
fastest path for medium tables. Results are uploaded back via
`load_table_from_dataframe` (Parquet upload under the hood).

For very large tables, consider aggregating in SQL first. `dbt-burla` does
not yet stream parquet files directly to/from Burla's mounted NFS for you;
that's on the roadmap.

## Running SQL models

SQL models run in BigQuery as usual. No difference in behavior from vanilla
`dbt-bigquery`.

## Costs

Three cost sources:

1. **BigQuery query bytes** for reading upstream and writing results.
2. **BigQuery Storage API charges** for large reads.
3. **Burla worker time** for the Python execution.

For a lot of Python workloads (per-row API calls, LLM inference) the Burla
compute dominates - but for very chatty small transforms, the BQ I/O can
surprise you.

## Known caveats

- Partitioned/clustered tables: the target table inherits partitioning
  only if configured in the model config (via standard dbt config).
  `load_table_from_dataframe` respects partitioning metadata set on the
  existing table when `WRITE_APPEND` is used.
- Regional datasets: set `location:` in your profile; Burla workers are
  region-agnostic, but the BigQuery read/write must match the dataset.
