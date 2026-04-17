{{ config(materialized='view') }}

select
    snapshot_date,
    snapshot_ts,
    source_uri,
    email,
    name,
    type,
    stripe_plan,
    cancel_date,
    start_date,
    expiration_date,
    first_paid_date,
    imputed_price_usd                   as revenue,
    country,
    paid_source,
    free_source,
    activity
from {{ ref('fct__substack_subscriber_daily') }}