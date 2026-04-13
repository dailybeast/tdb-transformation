{{ config(materialized='table') }}

with source as (
    select
        id                      as customer_id,
        email,
        name,
        created                 as customer_created_at,
        delinquent
    from {{ source('stripe', 'customer') }}
)

select * from source