{{ config(materialized='table') }}

with subscribers as (
    select *
    from {{ ref('stg__substack_subscribers') }}
),

stripe_invoices as (
    select
        subscription_id,
        period_start,
        period_end,
        date_diff(date(period_end), date(period_start), day) as period_days
    from {{ ref('stg__stripe_invoices') }}
    where subscription_id is not null
    qualify row_number() over (
        partition by subscription_id order by invoice_created_at desc
    ) = 1
),

stripe_subs as (
    select
        s.subscription_id               as stripe_subscription_id,
        c.email,
        case
            when i.period_days > 300 then 'annual'
            else 'monthly'
        end                             as stripe_billing_interval,
        s.status                        as stripe_status,
        i.period_start                  as current_period_start,
        i.period_end                    as current_period_end,
        s.cancel_at_period_end,
        s.canceled_at
    from {{ ref('stg__stripe_subscriptions') }} s
    left join {{ ref('stg__stripe_customers') }} c
        on s.customer_id = c.customer_id
    left join stripe_invoices i
        on i.subscription_id = s.subscription_id
    qualify row_number() over (
        partition by lower(trim(c.email))
        order by s._fivetran_start desc
    ) = 1
)

select
    -- substack spine
    ss.snapshot_date,
    ss.publication,
    ss.subscription_id,
    ss.user_id,
    ss.email,
    ss.subscription_interval,
    ss.stripe_plan,
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
    )                                   as is_non_stripe_paid

from subscribers ss
left join stripe_subs str
    on lower(trim(ss.email)) = lower(trim(str.email))
