"""Classify support tickets with an LLM, fanned out across Burla workers."""

import json
import os

import pandas as pd
from burla import remote_parallel_map


def _classify(row: dict) -> dict:
    """Run on one Burla worker; calls the OpenAI API for one ticket."""
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError(
            "openai package is required. Add it to the Burla worker image "
            "or include it in dbt's Python requirements."
        ) from exc

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Classify the support ticket. Return strict JSON "
                        "with fields: category (one of: billing, "
                        "technical, sales, other), sentiment (positive, "
                        "neutral, negative)."
                    ),
                },
                {"role": "user", "content": f"Subject: {row['subject']}\n\n{row['body']}"},
            ],
            response_format={"type": "json_object"},
            timeout=30,
        )
        parsed = json.loads(response.choices[0].message.content or "{}")
        return {
            **row,
            "category": parsed.get("category"),
            "sentiment": parsed.get("sentiment"),
            "error": None,
        }
    except Exception as exc:  # keep the row, mark it failed
        return {**row, "category": None, "sentiment": None, "error": str(exc)}


def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=50,
        burla_cpus_per_worker=1,
        # Bake `openai` into a custom image for lower cold-start latency:
        # burla_image="us-docker.pkg.dev/my-proj/llm-worker:latest",
    )

    tickets = dbt.ref("stg_support_tickets").to_dict("records")
    classified = list(remote_parallel_map(_classify, tickets))
    return pd.DataFrame(classified)
