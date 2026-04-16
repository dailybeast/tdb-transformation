{{ config(materialized='table') }}

with source as (
    select
        id              as subscription_item_id,
        subscription_id,
        plan_id,
        created,
        _fivetran_synced
    from {{ source('stripe', 'subscription_item') }}
)

select * from source
