# `dbt-burla` examples

Each subfolder is a standalone dbt project illustrating one flavor of
running Python models on Burla.

| Example | Warehouse | What it shows |
|---------|-----------|---------------|
| [`01-quickstart-duckdb`](01-quickstart-duckdb/) | DuckDB | Zero-setup run of `dbt-burla` in-process |
| [`02-snowflake-ml-scoring`](02-snowflake-ml-scoring/) | Snowflake | Per-row ML inference fanned out on Burla |
| [`03-bigquery-llm-enrichment`](03-bigquery-llm-enrichment/) | BigQuery | LLM API call per row across a cluster |
| [`04-fan-out-heavy-compute`](04-fan-out-heavy-compute/) | DuckDB | Using `remote_parallel_map` *inside* a model for massive fan-out |

All examples default to `burla_fake: true` so you can run them without a
Burla cluster. To run them on a real cluster, set `burla_fake: false` in the
local `profiles.yml` and make sure you've run `burla login`.

Each example has its own README with setup instructions.
