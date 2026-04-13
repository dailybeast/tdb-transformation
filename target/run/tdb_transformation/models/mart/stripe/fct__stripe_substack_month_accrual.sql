
  
    

    create or replace table `data-platform-455517`.`stripe`.`fct__stripe_substack_month_accrual`
      
    
    

    
    OPTIONS(
      description="""Monthly grain revenue accrual for Substack Stripe subscriptions. One row per reporting_month \u00d7 revenue_type \u00d7 billing_interval. Aggregates recognized revenue from fct__stripe_substack_charge_accrual. A typical month produces three rows: monthly subscriber, annual subscriber, and app_store. Reporting month follows the 16th-to-15th financial calendar. Use this model for month-over-month revenue trending and finance reporting. Feed fct__stripe_substack_month_revenue_projections for forward projections.\n"""
    )
    as (
      

with subscriber_charges as (
    select
        reporting_month,
        billing_interval,
        'subscriber'                    as revenue_type,
        sum(recognized_revenue_usd)     as recognized_revenue_usd,
        count(distinct email)           as subscriber_count,
        count(distinct charge_id)       as charge_count
    from `data-platform-455517`.`stripe`.`int__stripe_substack_charges`
    group by 1, 2, 3
),

appstore_payouts as (
    select
        reporting_month,
        cast(null as string)            as billing_interval,
        'app_store'                     as revenue_type,
        sum(net_amount_usd)             as recognized_revenue_usd,
        cast(null as int64)             as subscriber_count,
        count(distinct charge_id)       as charge_count
    from `data-platform-455517`.`stripe`.`int__stripe_appstore_payouts`
    group by 1, 2, 3
),

combined as (
    select * from subscriber_charges
    union all
    select * from appstore_payouts
)

select
    reporting_month,
    date_add(
        date_sub(date_trunc(parse_date('%B %Y', reporting_month), month), interval 1 month),
        interval 15 day
    )                                   as reporting_month_start,
    date_add(
        date_trunc(parse_date('%B %Y', reporting_month), month),
        interval 14 day
    )                                   as reporting_month_end,
    revenue_type,
    billing_interval,
    recognized_revenue_usd,
    subscriber_count,
    charge_count
from combined
    );
  