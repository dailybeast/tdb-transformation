{{ config(materialized='table') }}

with source as (
    select
        id              as plan_id,
        `interval`      as billing_interval,
        interval_count,
        amount / 100.0  as amount_usd,
        currency,
        active
    from {{ source('stripe', 'plan') }}
)

select * from source
