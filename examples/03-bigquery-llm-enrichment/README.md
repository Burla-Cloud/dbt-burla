# BigQuery + Burla: per-row LLM enrichment

Call an LLM on every row of a BigQuery table, in parallel, from inside a
dbt Python model. Great for classification, entity extraction, summarization,
or vector embeddings over large tables.

## Setup

```bash
pip install "dbt-burla[bigquery]" openai
```

Copy the profile template and fill in your GCP project:

```bash
cp profiles.example.yml profiles.yml
$EDITOR profiles.yml
```

Make sure `gcloud auth application-default login` or a service account
key is configured, and `burla login` is set up (or set `burla_fake: true`
to run locally).

Set your LLM API key:

```bash
export OPENAI_API_KEY=sk-...
```

## Run it

```bash
dbt run --profiles-dir .
```

## What it does

`categorized_tickets.py` reads every row of `stg_support_tickets`, fans the
rows out to Burla, and each worker calls the OpenAI API to classify the
ticket's category and sentiment. Results are written back to BigQuery.

```python
from burla import remote_parallel_map

def classify(row):
    from openai import OpenAI
    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Classify the support ticket..."},
            {"role": "user", "content": row["body"]},
        ],
        response_format={"type": "json_object"},
    )
    ...
```

Each Burla worker is isolated, so API keys, rate limits, and failures
per-row don't interfere. If a row fails, only that row's record is marked
failed - the rest of the model succeeds.

## Cost control

- `burla_workers` caps concurrent LLM API calls - tune to stay under your
  provider's rate limit.
- `burla_cpus_per_worker=1` - LLM calls are I/O bound, so small machines
  are cheapest.
