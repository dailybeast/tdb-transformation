
  
    

    create or replace table `data-platform-455517`.`stripe`.`fct__stripe_monthly_accrual`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with subscriber_charges as (
    select  
        charge_id,
        subscription_id,
        customer_id,
        email,
        billing_interval,
        ss_subscription_interval,
        charged_at,
        charge_currency,
        exchange_rate,
        settled_amount_usd,
        net_amount_usd,
        reporting_month,
        recognized_revenue_usd,
        month_offset,
        'subscriber' as revenue_type
    from `data-platform-455517`.`stripe`.`int__stripe_substack_charges`
), 


appstore_payouts as (
    select
        charge_id,
        cast(null as string)    as subscription_id,
        cast(null as string)    as customer_id,
        cast(null as string)    as email,
        cast(null as string)    as billing_interval,
        cast(null as string)    as ss_subscription_interval,
        charged_at,
        charge_currency,
        cast(null as float64)   as exchange_rate,
        settled_amount_usd,
        net_amount_usd,
        reporting_month,
        net_amount_usd          as recognized_revenue_usd,
        0                       as month_offset,
        'app_store'             as revenue_type
    from `data-platform-455517`.`stripe`.`int__stripe_appstore_payouts`
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
        date_sub(date_trunc(parse_date('%B %Y', reporting_month), month), interval 1 month),
        interval 15 day
    )                           as reporting_month_start,
    date_add(
        date_trunc(parse_date('%B %Y', reporting_month), month),
        interval 14 day
    )                           as reporting_month_end
from combined
    );
  