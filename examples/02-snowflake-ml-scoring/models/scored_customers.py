"""Score every customer through an ML model fanned out across Burla.

Pattern: load a model from your artifact store inside `score()`, then call
`remote_parallel_map` to run it row-by-row across hundreds of VMs.
"""

import math

import pandas as pd
from burla import remote_parallel_map


def score(row: dict) -> dict:
    """Called on one Burla worker for one customer row.

    In a real project this would load a cached sklearn/xgboost/torch model
    and call `model.predict_proba(...)`. We simulate one here for demonstration.
    """
    spend = float(row["total_spend"] or 0)
    orders = int(row["order_count"] or 0)
    recency = int(row["days_since_last_order"] or 365)
    churn_score = 1 - math.exp(-recency / 90) + 0.1 * math.exp(-orders / 5)
    return {**row, "churn_score": round(min(churn_score, 1.0), 4)}


def model(dbt, session):
    dbt.config(
        materialized="table",
        burla_workers=500,
        burla_cpus_per_worker=2,
        # burla_image="us-docker.pkg.dev/my-proj/my-model:latest",
    )

    customers = dbt.ref("stg_customers").to_dict("records")
    scored = list(remote_parallel_map(score, customers))
    return pd.DataFrame(scored)
