def model(dbt, session):
    dbt.config(materialized="table")
    orders = dbt.ref("stg_orders")
    enriched = orders.assign(
        amount_cents=(orders["amount"] * 100).astype(int),
        is_large=orders["amount"] > 100,
        greeting=orders["customer_name"].map(lambda n: f"Hello, {n}!"),
    )
    return enriched
