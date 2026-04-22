"""A Python model that enriches orders with computed columns."""

import pandas as pd


def model(dbt, session):
    dbt.config(
        materialized="table",
        # Tune for your workload. These apply to real Burla runs; they're
        # no-ops in fake mode.
        burla_workers=8,
        burla_cpus_per_worker=1,
    )

    orders: pd.DataFrame = dbt.ref("stg_orders")

    bucket_by_amount = pd.cut(
        orders["amount"],
        bins=[-1, 50, 200, float("inf")],
        labels=["small", "medium", "large"],
    )

    return orders.assign(
        amount_bucket=bucket_by_amount.astype(str),
        amount_cents=(orders["amount"] * 100).round().astype(int),
        is_us=orders["country"] == "US",
    )
