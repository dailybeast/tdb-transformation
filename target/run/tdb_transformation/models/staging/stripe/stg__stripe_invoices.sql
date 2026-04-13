
  
    

    create or replace table `data-platform-455517`.`stripe`.`stg__stripe_invoices`
      
    
    

    
    OPTIONS(
      description="""One row per Stripe invoice. Bridges charges to subscriptions and provides period_start / period_end used to infer billing interval (monthly vs annual) in the intermediate layer.\n"""
    )
    as (
      

with source as (
    select
        id                      as invoice_id,
        subscription_id,
        customer_id,
        created                 as invoice_created_at,
        period_start,
        period_end,
        amount_paid / 100.0     as amount_paid_usd,
        lower(currency)         as currency,
        status
    from `ai-mvp-392019`.`stripe`.`invoice`
)

select * from source
    );
  