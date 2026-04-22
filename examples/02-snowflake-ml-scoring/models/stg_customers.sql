{{ config(materialized="view") }}

select
    customer_id,
    total_spend,
    order_count,
    days_since_last_order
from {{ source('raw', 'customers') }}
