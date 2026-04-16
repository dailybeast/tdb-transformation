{{ config(materialized='view') }}

select
    snapshot_date,
    publication,
    email,
    name,
    is_gift,
    billing_interval,
    is_comp,
    subscription_interval,
    start_date,
    first_paid_date,
    cancel_date,
    expiration_date,
    imputed_price_usd                   as revenue,
    type_bucket                         as type,
    status_bucket                       as status,
    is_active_paid
from {{ ref('fct__substack_subscriber_daily') }}
