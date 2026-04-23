# Troubleshooting

> **When to read this page:** something broke. Start here before opening an issue - most problems are one of the patterns below.

Common issues and fixes.

## "Could not find adapter type burla_*"

`pip install "dbt-burla[<warehouse>]"` didn't complete, or you're in a
different virtualenv than `dbt` itself. Verify:

```bash
python -c "from dbt.adapters.burla_duckdb import Plugin; print(Plugin)"
```

## "IndentationError: unexpected indent" during a Python model

dbt emits compiled code with leading whitespace; `dbt-burla` strips this
automatically (via `compiled_code.lstrip()` in `BurlaPythonJobHelper.submit`).
If you're seeing this, you're on a very old version - upgrade.

## "The `burla` client isn't installed"

`pip install burla` (should be pulled in automatically by `dbt-burla`).
Or set `burla_fake: true` in your profile to run Python models in-process.

## "Burla cluster unreachable at ..."

Check:

1. `burla login` was run and credentials are in `~/.config/burla/`.
2. `burla_cluster_url` in your profile matches your cluster.
3. `BURLA_CLUSTER_DASHBOARD_URL` env var isn't set to something stale.

A quick sanity check:

```bash
python -c "from burla import get_cluster_dashboard_url; print(get_cluster_dashboard_url())"
```

## "Python model must return a pandas DataFrame; got ..."

Your `model()` function must return a `pd.DataFrame`. Common mistakes:

- Returning a `list` or `dict` - wrap with `pd.DataFrame(...)`.
- Returning a Polars / pyarrow object - convert first: `df.to_pandas()`.
- Forgetting the `return` statement.

## "Model `X` referenced upstream relation `...` which was not pre-loaded"

dbt didn't include the relation in the compiled `refs`/`sources` dict.
This usually means:

- You're using a non-standard `dbt.ref()` pattern (e.g., constructing the
  name dynamically). Use static strings so dbt can resolve at compile time.
- There's a `ref` that exists in dbt's DAG but isn't reachable by your
  model's `depends_on`. Re-check your `schema.yml` / `ref()` calls.

## "Catalog Error: Table with name X__dbt_tmp does not exist"

You're on an old version of `dbt-burla` that hadn't shipped its custom
DuckDB table materialization. Upgrade to `>=0.1.0`.

## Python model runs forever / hangs on Burla

Likely culprits:

- The Burla cluster is cold-starting workers (first run takes longer than
  subsequent ones).
- Your `burla_image` is large and being pulled.
- Your `model()` is doing a synchronous network call without a timeout -
  add `timeout=30` to any HTTP calls inside `remote_parallel_map` callbacks.

Set `burla_timeout_s` on the model to fail fast instead of hanging.

## CI pytest fails with "ResourceWarning: unclosed file"

Add this to your `pyproject.toml`:

```toml
[tool.pytest.ini_options]
filterwarnings = [
    "ignore::ResourceWarning",
    "ignore::pytest.PytestUnraisableExceptionWarning",
]
```

These come from dbt-core's own log file handling, not your code.

## Snowflake: "100078 (22000): Binding data in type (...) is not supported"

`write_pandas` can't handle every pandas dtype. Convert to str/int/float
before returning:

```python
df["my_col"] = df["my_col"].astype(str)
return df
```

## BigQuery: "Access Denied" during `load_table_from_dataframe`

Your service account needs both `bigquery.dataEditor` and
`bigquery.jobUser` on the target dataset/project. `dbt debug` doesn't
catch this because it only tests SELECT permissions.

## Still stuck?

Open an issue at [github.com/Burla-Cloud/dbt-burla/issues](https://github.com/Burla-Cloud/dbt-burla/issues)
with:

- `dbt --version`
- Full `dbt run --debug` output
- Your Python model (redacted if needed)
- Warehouse variant
