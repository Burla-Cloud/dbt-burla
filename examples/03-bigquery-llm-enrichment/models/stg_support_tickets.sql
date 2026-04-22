{{ config(materialized="view") }}

select
    ticket_id,
    created_at,
    subject,
    body
from {{ source('raw', 'support_tickets') }}
