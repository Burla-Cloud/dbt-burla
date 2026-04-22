{{ config(materialized="view") }}

select 1 as id, 'alice'   as customer, 120.50 as amount, 'US' as country
union all
select 2 as id, 'bob'     as customer,  40.00 as amount, 'UK' as country
union all
select 3 as id, 'carol'   as customer, 500.00 as amount, 'US' as country
union all
select 4 as id, 'dave'    as customer,   5.00 as amount, 'DE' as country
union all
select 5 as id, 'eve'     as customer, 250.00 as amount, 'US' as country
