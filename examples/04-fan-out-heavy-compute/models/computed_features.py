"""Massive fan-out with `remote_parallel_map` inside a dbt Python model."""

import random

import pandas as pd
from burla import remote_parallel_map


def _monte_carlo_pi(row: dict) -> dict:
    """Per-worker: estimate pi using the row's sample count."""
    n = int(row["n_samples"])
    rng = random.Random(row["id"])
    inside = 0
    for _ in range(n):
        x, y = rng.random(), rng.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return {**row, "pi_estimate": 4.0 * inside / max(n, 1), "error": None}


def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=1000,
        burla_cpus_per_worker=4,
    )

    inputs = dbt.ref("stg_inputs").to_dict("records")
    results = list(remote_parallel_map(_monte_carlo_pi, inputs))
    return pd.DataFrame(results)
