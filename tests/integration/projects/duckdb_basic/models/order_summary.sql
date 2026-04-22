{#
  Downstream SQL model that reads from the Python model's output -
  verifies that the python table was materialized in a queryable form.
#}
select
    sum(amount) as total_amount,
    sum(amount_cents) as total_amount_cents,
    sum(case when is_large then 1 else 0 end) as large_order_count,
    count(*) as row_count
from {{ ref('enriched_orders') }}
