{{ config(materialized="view") }}

select 1 as id, 'alice' as customer_name, 120.50 as amount
union all
select 2 as id, 'bob' as customer_name, 40.00 as amount
union all
select 3 as id, 'carol' as customer_name, 500.00 as amount
union all
select 4 as id, 'dave' as customer_name, 5.00 as amount
