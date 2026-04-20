{{ config(materialized='table') }}

with subscribers as (
    select *
    from {{ ref('stg__substack_subscribers') }}
),

stripe_subs as (
    select
        subscription_id                 as stripe_subscription_id,
        email,
        billing_interval                as stripe_billing_interval,
        status                          as stripe_status,
        current_period_start,
        current_period_end,
        cancel_at_period_end,
        canceled_at
    from {{ ref('int__stripe_substack_subscriptions') }}
    qualify row_number() over (
        partition by lower(trim(email))
        order by subscription_created_at desc
    ) = 1
)

select
    -- substack spine
    ss.snapshot_date,
    ss.snapshot_ts,
    ss.source_uri,
    ss.publication,
    ss.subscription_id,
    ss.user_id,
    ss.email,
    ss.name,
    ss.type,
    ss.subscription_interval,
    ss.stripe_plan,
    ss.country,
    ss.paid_source,
    ss.free_source,
    ss.is_subscribed,
    ss.is_comp,
    ss.is_gift,
    ss.is_free_trial,
    ss.activity,
    ss.start_date,
    ss.first_paid_date,
    ss.expiration_date,
    ss.cancel_date,
    -- stripe enrichment
    str.stripe_subscription_id,
    str.stripe_status,
    str.current_period_start,
    str.current_period_end,
    str.cancel_at_period_end,
    str.canceled_at,
    -- derived
    coalesce(
        str.stripe_billing_interval,
        case ss.subscription_interval
            when 'annual'  then 'annual'
            when 'monthly' then 'monthly'
        end,
        case
            when lower(ss.stripe_plan) like '%year%'  then 'annual'
            when lower(ss.stripe_plan) like '%month%' then 'monthly'
        end
    )                                   as billing_interval,

    safe_cast(
        regexp_extract(ss.stripe_plan, r'\$(\d+(?:\.\d+)?)')
        as float64
    )                                   as imputed_price_usd,

    (
        ss.first_paid_date is not null
        and str.stripe_subscription_id is null
    )                                   as is_non_stripe_paid,

    case
        when ss.paid_source = 'substack-ios-in-app-purchase' then 'ios'
        when ss.is_comp                                      then 'comp'
        when ss.is_gift                                      then 'gift'
        when str.stripe_subscription_id is not null          then 'stripe'
        else                                                      'free'
    end                                                      as payer_type

from subscribers ss
left join stripe_subs str
    on lower(trim(ss.email)) = lower(trim(str.email))
