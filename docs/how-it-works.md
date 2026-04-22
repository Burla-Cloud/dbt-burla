# How it works

A deep dive into what actually happens when `dbt run` hits a Python model
in a `dbt-burla` project.

## The pieces

`dbt-burla` ships three dbt adapters - `burla_duckdb`, `burla_snowflake`,
and `burla_bigquery` - each a subclass of the matching warehouse adapter.
SQL runs unchanged. Python models are intercepted and routed through
`BurlaPythonJobHelper`.

```text
                    ┌────────────────────────┐
                    │    dbt run CLI         │
                    └────────────┬───────────┘
                                 ▼
                    ┌────────────────────────┐
                    │    BurlaXxxAdapter     │
                    │  (subclasses the       │
                    │   warehouse adapter)   │
                    └────┬───────────────┬───┘
                         │               │
                SQL ─────┘               └───── Python
                │                                  │
                ▼                                  ▼
     ┌────────────────────┐        ┌──────────────────────────┐
     │ warehouse adapter  │        │  BurlaPythonJobHelper    │
     │ (dbt-duckdb /      │        │                          │
     │  dbt-snowflake /   │        │  1. read upstream refs   │──► warehouse
     │  dbt-bigquery)     │        │  2. run model on Burla   │──► Burla cluster
     └────────────────────┘        │  3. write df back        │──► warehouse
                                   └──────────────────────────┘
```

## Python model lifecycle, step by step

Here's the full journey of `enriched_orders.py` when you run `dbt run`:

### 1. dbt compiles the model

dbt renders your Python file into `compiled_code`. The compiled form looks
roughly like this (abbreviated):

```python
def model(dbt, session):
    orders = dbt.ref("stg_orders")
    return orders.assign(...)


# Appended by dbt:
def ref(*args, **kwargs):
    refs = {"stg_orders": '"db"."schema"."stg_orders"'}
    ...

def source(*args, dbt_load_df_function):
    sources = {}
    ...

class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.ref = ...
        self.source = ...
        self.is_incremental = False
        self.this = this()
```

The key bit: **dbt bakes a `{ref_name: resolved_relation_string}` dict**
into the compiled code. `BurlaPythonJobHelper` reads it via AST.

### 2. `submit_python_job` is called

`BurlaXxxAdapter.submit_python_job(parsed_model, compiled_code)` runs. It
hands off to `BurlaPythonJobHelper.for_adapter(self, parsed_model)` and
then to `helper.submit(compiled_code)`.

### 3. Helper extracts upstream relations

`_extract_relation_maps(compiled_code)` walks the AST of the compiled code
and pulls out the literal `refs = {...}` and `sources = {...}` dicts.

### 4. Helper loads each upstream as a DataFrame

For each unique relation string, the warehouse backend
(`DuckDBBackend` / `SnowflakeBackend` / `BigQueryBackend`) runs a
`select * from <relation>` and converts the result to a pandas DataFrame.
These DataFrames are held in memory on the dbt-running machine.

### 5. Helper runs the model

Two paths:

**Fake mode (`burla_fake: true`).** The helper executes the compiled code
in-process. It constructs `dbtObj(load_df)` where `load_df(rel_str)`
returns the pre-loaded DataFrame, then calls `model(dbt, None)`. Anything
the user's code does with `remote_parallel_map` runs in-process too (the
helper installs a local fallback for the duration of the call).

**Real mode (`burla_fake: false`).** The helper packages `compiled_code`
plus the upstream DataFrames into a cloudpickleable closure and submits it
as a single input to `burla.remote_parallel_map(..., [None])`. Burla runs
the closure on one worker, which constructs `dbtObj` the same way and
calls `model(dbt, None)`. If the user's model body calls
`remote_parallel_map` itself, that runs on further Burla workers -
standard nested fan-out.

### 6. Helper validates and writes the result

The returned object must be a `pandas.DataFrame`. The helper writes it back
to the warehouse via `WarehouseBackend.write_from_dataframe(df, target, mode=...)`:

- `materialized: table` → `mode="replace"` (atomic `CREATE OR REPLACE`)
- `materialized: incremental` → `mode="append"`

### 7. dbt marks the node complete

The helper returns `{"rows": N, "runtime_s": T}`; dbt records the success
and moves on to the next node.

## Where the data flows

```text
                      dbt-running machine              Burla cluster
                    ┌───────────────────────┐      ┌──────────────────┐
  warehouse ──────► │ helper pulls upstream │      │                  │
                    │   → pandas DataFrame  │      │                  │
                    │                       │ ──►  │  model(dbt, None)│
                    │   cloudpickle + data  │      │   runs on worker │
                    │                       │ ◄──  │                  │
                    │   df returned locally │      │                  │
  warehouse ◄────── │ helper writes back    │      │                  │
                    └───────────────────────┘      └──────────────────┘
```

For small-to-medium upstream tables (say, <1M rows) this round-trip is
fine. For very large tables you'll want to do the aggregation in SQL first
so the Python model receives a manageable input.

## Why a subclass per warehouse

dbt's adapter discovery is module-name-based: `type: burla_snowflake` in
`profiles.yml` causes dbt to `import dbt.adapters.burla_snowflake`. Rather
than wrapping warehouse adapters at runtime (which is fragile), we ship
three sibling adapter modules that each inherit from their warehouse's
native adapter. This gives us:

- Every SQL feature of the underlying adapter works automatically
- Materializations, relations, columns, incremental strategies, etc.
  are inherited
- No monkey-patching or dynamic dispatch
- Clean `pip install "dbt-burla[<warehouse>]"` story via extras

See [`src/dbt/adapters/burla/python_submissions.py`](https://github.com/Burla-Cloud/dbt-burla/blob/main/src/dbt/adapters/burla/python_submissions.py)
for the full helper implementation.
