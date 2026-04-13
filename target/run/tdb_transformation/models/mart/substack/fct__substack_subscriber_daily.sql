
   
      -- generated script to merge partitions into `data-platform-455517`.`substack`.`fct__substack_subscriber_daily`
      declare dbt_partitions_for_replacement array<timestamp>;

      
      
       -- 1. create a temp table with model data
        
  
    

    create or replace table `data-platform-455517`.`substack`.`fct__substack_subscriber_daily__dbt_tmp`
      
    partition by timestamp_trunc(snapshot_date, day)
    

    
    OPTIONS(
      description="""""",
    
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      

with base as (
    select * 
    from `data-platform-455517`.`substack`.`int__substack_subscriber_daily`
    
    where date(snapshot_date) >= date_sub(current_date(), interval 3 day)
    
), 

buckets as (
    select
        *,

        case
            when is_gift and billing_interval = 'monthly'  then 'Monthly Gift'
            when is_gift and billing_interval = 'annual'   then 'Yearly Gift'
            when subscription_interval = 'lifetime'        then 'Royal Tier'
            when is_comp and billing_interval = 'annual'   then 'Yearly Subscriber'
            when is_comp and billing_interval = 'monthly'  then 'Monthly Subscriber'
            when is_comp                                   then 'Comp'
            when billing_interval = 'annual'               then 'Yearly Subscriber'
            when billing_interval = 'monthly'              then 'Monthly Subscriber'
            else 'Other'
        end as type_bucket,

        case
            when is_gift then
                case
                    when subscription_expires_at > snapshot_date then 'Active'
                    else 'Expired'
                end
            when first_payment_at is null and not coalesce(is_comp, false) then 'Non-paid'
            when unsubscribed_at is not null then
                case
                    when subscription_expires_at > snapshot_date then 'Cancelled but Active'
                    else 'Expired'
                end
            when subscription_expires_at is null         then 'Expired'
            when subscription_expires_at > snapshot_date then 'Active'
            else 'Expired'
        end as status_bucket

    from base
)

select
    *,
    status_bucket in ('Active', 'Cancelled but Active') as is_active_paid
from buckets
    );
  
      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
              array_agg(distinct timestamp_trunc(snapshot_date, day) IGNORE NULLS)
          from `data-platform-455517`.`substack`.`fct__substack_subscriber_daily__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `data-platform-455517`.`substack`.`fct__substack_subscriber_daily` as DBT_INTERNAL_DEST
        using (
        select
        * from `data-platform-455517`.`substack`.`fct__substack_subscriber_daily__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and timestamp_trunc(DBT_INTERNAL_DEST.snapshot_date, day) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`snapshot_date`, `publication`, `subscription_id`, `user_id`, `email`, `subscription_interval`, `stripe_plan_name`, `paid_attribution`, `free_attribution`, `is_subscribed`, `is_comp`, `is_gift`, `is_free_trial`, `activity_rating`, `subscription_created_at`, `first_payment_at`, `subscription_expires_at`, `unsubscribed_at`, `stripe_subscription_id`, `stripe_status`, `current_period_start`, `current_period_end`, `cancel_at_period_end`, `canceled_at`, `billing_interval`, `imputed_price_usd`, `is_non_stripe_paid`, `type_bucket`, `status_bucket`, `is_active_paid`)
    values
        (`snapshot_date`, `publication`, `subscription_id`, `user_id`, `email`, `subscription_interval`, `stripe_plan_name`, `paid_attribution`, `free_attribution`, `is_subscribed`, `is_comp`, `is_gift`, `is_free_trial`, `activity_rating`, `subscription_created_at`, `first_payment_at`, `subscription_expires_at`, `unsubscribed_at`, `stripe_subscription_id`, `stripe_status`, `current_period_start`, `current_period_end`, `cancel_at_period_end`, `canceled_at`, `billing_interval`, `imputed_price_usd`, `is_non_stripe_paid`, `type_bucket`, `status_bucket`, `is_active_paid`)

;

      -- 4. clean up the temp table
      drop table if exists `data-platform-455517`.`substack`.`fct__substack_subscriber_daily__dbt_tmp`

  


  

    