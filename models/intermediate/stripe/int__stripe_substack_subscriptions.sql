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
<<<<<<< HEAD
    select 
        customer_id, 
=======
    select
        customer_id,
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        email
    from {{ ref('stg__stripe_customers') }}
),

invoices as (
    select
        i.subscription_id,
        i.period_start,
<<<<<<< HEAD
        i.period_end, 
        date_diff(date(period_end), date(period_start), day) as period_days,
=======
        i.period_end,
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
        c.settled_amount_usd as amount_paid_usd,
        c.net_amount_usd
    from {{ ref('stg__stripe_invoices') }} i
    left join {{ ref('stg__stripe_charges') }} c
        on c.invoice_id = i.invoice_id
    where i.subscription_id is not null
    qualify row_number() over (partition by i.subscription_id order by i.invoice_created_at desc) = 1
<<<<<<< HEAD
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
        case
            when i.period_days > 300     then 'annual'
            when i.amount_paid_usd >= 50 then 'annual'
        else 'monthly' end as billing_interval,
        i.amount_paid_usd as last_invoice_amount_usd
    from subscriptions s
    left join customers c
        on s.customer_id = c.customer_id
    left join invoices i 
        on i.subscription_id = s.subscription_id
=======
),

plan_interval as (
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
    coalesce(pi.billing_interval, 'monthly') as billing_interval,
    i.amount_paid_usd as last_invoice_amount_usd
from subscriptions s
left join customers c
    on s.customer_id = c.customer_id
left join invoices i
    on i.subscription_id = s.subscription_id
left join plan_interval pi
    on pi.subscription_id = s.subscription_id
>>>>>>> d009ab0aae36c911fb8cd277bf018397fb72f3fd
