{{ config(materialized='table') }}

with subscriptions as (
    select
        s.*,
        c.email
    from {{ ref('stg__stripe_subscriptions') }} s
    left join {{ ref('stg__stripe_customers') }} c
        on s.customer_id = c.customer_id
    left join (
        select subscription_id, period_start, period_end
        from {{ ref('stg__stripe_invoices') }}
        where subscription_id is not null
        qualify row_number() over (
            partition by subscription_id
            order by invoice_created_at desc
        ) = 1
    ) i
        on i.subscription_id = s.subscription_id
    qualify row_number() over (
        partition by s.subscription_id
        order by s._fivetran_start desc
    ) = 1
),

customers as (
    select
        customer_id,
        email
    from {{ ref('stg__stripe_customers') }}
),

invoices as (
    select
        i.subscription_id,
        i.period_start,
        i.period_end,
        date_diff(date(period_end), date(period_start), day) as period_days,
        c.settled_amount_usd as amount_paid_usd,
        c.net_amount_usd
    from {{ ref('stg__stripe_invoices') }} i
    left join {{ ref('stg__stripe_charges') }} c
        on c.invoice_id = i.invoice_id
    where i.subscription_id is not null
    qualify row_number() over (partition by i.subscription_id order by i.invoice_created_at desc) = 1
),

subscription_plans as (
    select
        si.subscription_id,
        case p.billing_interval
            when 'year'  then 'annual'
            when 'month' then 'monthly'
            else 'monthly'
        end as billing_interval
    from {{ ref('stg__stripe_subscription_items') }} si
    join {{ ref('stg__stripe_plans') }} p
        on p.plan_id = si.plan_id
    qualify row_number() over (
        partition by si.subscription_id
        order by si.created desc
    ) = 1
)

    select
        s.subscription_id,
        s.customer_id,
        c.email,
        s.status,
        i.period_start as current_period_start,
        i.period_end   as current_period_end,
        s.cancel_at_period_end,
        s.canceled_at,
        s.ended_at,
        s.subscription_created_at,
        coalesce(sp.billing_interval, 'monthly') as billing_interval,
        i.amount_paid_usd as last_invoice_amount_usd
    from subscriptions s
    left join customers c
        on s.customer_id = c.customer_id
    left join invoices i
        on i.subscription_id = s.subscription_id
    left join subscription_plans sp
        on sp.subscription_id = s.subscription_id
