
  
    

    create or replace table `data-platform-455517`.`stripe`.`int__stripe_substack_subscriptions`
      
    
    

    
    OPTIONS(
      description="""Active Stripe subscriptions enriched with customer email and billing interval. One row per active subscription. Billing interval inferred from latest invoice period length (>300 days = annual). Used as the Stripe join target for subscriber-level models.\n"""
    )
    as (
      

with subscriptions as (
    select
        s.*,
        c.email
    from `data-platform-455517`.`stripe`.`stg__stripe_subscriptions` s
    left join `data-platform-455517`.`stripe`.`stg__stripe_customers` c
        on s.customer_id = c.customer_id
    left join (
        select subscription_id, period_start, period_end
        from `data-platform-455517`.`stripe`.`stg__stripe_invoices`
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
    from `data-platform-455517`.`stripe`.`stg__stripe_customers`
),

invoices as (
    select
        i.subscription_id,
        i.period_start,
        i.period_end, 
        date_diff(date(period_end), date(period_start), day) as period_days,
        c.settled_amount_usd as amount_paid_usd,
        c.net_amount_usd
    from `data-platform-455517`.`stripe`.`stg__stripe_invoices` i
    left join `data-platform-455517`.`stripe`.`stg__stripe_charges` c
        on c.invoice_id = i.invoice_id
    where i.subscription_id is not null
    qualify row_number() over (partition by i.subscription_id order by i.invoice_created_at desc) = 1
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
    );
  