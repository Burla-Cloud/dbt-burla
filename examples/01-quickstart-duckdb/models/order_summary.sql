{#
  Downstream SQL model proves that the Python model's output is a normal
  warehouse table everything else in the DAG can use.
#}
select
    country,
    amount_bucket,
    count(*)         as order_count,
    sum(amount)      as total_amount
from {{ ref('enriched_orders') }}
group by 1, 2
order by country, amount_bucket
