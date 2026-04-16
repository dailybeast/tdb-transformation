{{ config(materialized='table') }}

with source as (
    select
        id                      as invoice_id,
        subscription_id,
        customer_id,
        created                 as invoice_created_at,
        period_start,
        period_end,
        amount_paid / 100.0     as amount_paid_usd,
        lower(currency)         as currency,
        status
    from {{ source('stripe', 'invoice') }}
)

select * from source
