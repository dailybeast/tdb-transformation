
  
    

    create or replace table `data-platform-455517`.`stripe`.`int__stripe_appstore_payouts`
      
    
    

    
    OPTIONS(
      description="""Apple App Store bulk subscription payouts received via Stripe. These are publication-level lump-sum payouts from Apple and cannot be linked to individual subscribers. Separated from subscriber charges for independent accrual treatment. Reporting month is derived from the payout description rather than the charge date.\n"""
    )
    as (
      

with source as (
    select *
    from `data-platform-455517`.`stripe`.`stg__stripe_charges`
    where description like 'Earnings from App Store subscriptions for%'
),
    
parsed as (
    select 
        charge_id,
        charged_at,
        description,
        settled_amount_usd,
        net_amount_usd,
        charge_currency,
        regexp_extract(description, r'for (.+)$') as payout_month_label,
        parse_date(
            '%B %Y',
            regexp_extract(description, r'for (.+)$')
        ) as payout_month_date
    from source
)

    select
        charge_id,
        charged_at,
        description,
        settled_amount_usd,
        net_amount_usd,
        charge_currency,
        payout_month_label,
        format_date('%B %Y', payout_month_date) as reporting_month,
        date_add(date_sub(payout_month_date, interval 1 month), interval 15 day) as reporting_month_start,
        date_add(payout_month_date, interval 14 day) as reporting_month_end
    from parsed
    );
  