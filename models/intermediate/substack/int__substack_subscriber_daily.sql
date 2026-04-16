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
    --substack spine
    ss.snapshot_date,
    ss.publication,
    ss.subscription_id,
    ss.user_id,
    ss.email,
    ss.subscription_interval,
    ss.stripe_plan_name,
    ss.paid_attribution,
    ss.free_attribution,
    ss.is_subscribed,
    ss.is_comp,
    ss.is_gift,
    ss.is_free_trial,
    ss.activity_rating,
    ss.subscription_created_at,
    ss.first_payment_at,
    ss.subscription_expires_at,
    ss.unsubscribed_at,
    --stripe enrichment
    str.stripe_subscription_id,
    str.stripe_status,
    str.current_period_start,
    str.current_period_end,
    str.cancel_at_period_end,
    str.canceled_at,
    --derived fields
<<<<<<< HEAD
    coalesce(
        str.stripe_billing_interval,
        case
            when lower(ss.stripe_plan_name) like '%year%'  then 'annual'
            when lower(ss.stripe_plan_name) like '%month%' then 'monthly'
        end
    )                                   as billing_interval,
=======
            case
                when ss.subscription_interval = 'annual'       then 'annual'
                when ss.subscription_interval = 'monthly'      then 'monthly'
                when str.stripe_billing_interval = 'annual'    then 'annual'
                else 'monthly'
            end                                                as billing_interval,
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd

    safe_cast(
        regexp_extract(ss.stripe_plan_name, r'\$(\d+(?:\.\d+)?)')
        as float64
    )                                   as imputed_price_usd,

    (
        ss.first_payment_at is not null
        and str.stripe_subscription_id is null
    )                                   as is_non_stripe_paid

    from subscribers ss
    left join stripe_subs str
        on lower(trim(ss.email)) = lower(trim(str.email))
group by all