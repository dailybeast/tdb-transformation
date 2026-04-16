{{ config(materialized='table') }}

with subscriber_charges as (
    select  
        charge_id,
        subscription_id,
        customer_id,
        email,
        billing_interval,
<<<<<<< HEAD
        ss_subscription_interval,
=======
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        charged_at,
        charge_currency,
        exchange_rate,
        settled_amount_usd,
        net_amount_usd,
        reporting_month,
        recognized_revenue_usd,
        month_offset,
<<<<<<< HEAD
=======
        recognition_date,
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        'subscriber' as revenue_type
    from {{ ref('int__stripe_substack_charges') }}
), 


appstore_payouts as (
    select
        charge_id,
        cast(null as string)    as subscription_id,
        cast(null as string)    as customer_id,
        cast(null as string)    as email,
        cast(null as string)    as billing_interval,
<<<<<<< HEAD
        cast(null as string)    as ss_subscription_interval,
=======
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        charged_at,
        charge_currency,
        cast(null as float64)   as exchange_rate,
        settled_amount_usd,
        net_amount_usd,
        reporting_month,
        net_amount_usd          as recognized_revenue_usd,
        0                       as month_offset,
<<<<<<< HEAD
=======
        date(charged_at)        as recognition_date,
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        'app_store'             as revenue_type
    from {{ ref('int__stripe_appstore_payouts') }}
),

combined as (
    select *
    from subscriber_charges
    UNION ALL
    select *
    from appstore_payouts
)

select
    *,
    date_add(
<<<<<<< HEAD
        date_sub(date_trunc(parse_date('%B %Y', reporting_month), month), interval 1 month),
        interval 15 day
    )                           as reporting_month_start,
    date_add(
        date_trunc(parse_date('%B %Y', reporting_month), month),
=======
        date_trunc(parse_date('%B %Y', reporting_month), month),
        interval 15 day
    )                           as reporting_month_start,
    date_add(
        date_add(date_trunc(parse_date('%B %Y', reporting_month), month), interval 1 month),
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        interval 14 day
    )                           as reporting_month_end
from combined