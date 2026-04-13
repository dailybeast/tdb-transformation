
  
    

    create or replace table `data-platform-455517`.`stripe`.`stg__stripe_subscriptions`
      
    
    

    
    OPTIONS(
      description="""One row per subscription state change from Fivetran's SCD2 subscription_history table. Includes both active and historical rows \u2014 use _fivetran_active = true to get current state. Intermediate layer is responsible for deduplication.\n"""
    )
    as (
      

with source as (
    select
        id                      as subscription_id,
        customer_id,
        status,
        current_period_start,
        current_period_end,
        cancel_at_period_end,
        cancel_at,
        canceled_at,
        ended_at,
        created                 as subscription_created_at,
        start_date,
        trial_start,
        trial_end,
        _fivetran_active,
        _fivetran_start,
        _fivetran_end
    from `ai-mvp-392019`.`stripe`.`subscription_history`
)

select * from source
    );
  