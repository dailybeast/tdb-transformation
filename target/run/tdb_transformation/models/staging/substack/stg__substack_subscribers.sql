
  
    

    create or replace table `data-platform-455517`.`substack`.`stg__substack_subscribers`
        
  (
    snapshot_date timestamp,
    publication string,
    subscription_id INT64,
    user_id INT64,
    is_subscribed boolean,
    is_comp boolean,
    is_gift boolean,
    is_free_trial boolean,
    subscription_interval string,
    activity_rating INT64,
    subscription_created_at timestamp,
    first_payment_at timestamp,
    subscription_expires_at timestamp,
    unsubscribed_at timestamp,
    total_count INT64,
    email string,
    stripe_plan_name string,
    paid_attribution string,
    free_attribution string
    
    )

      
    
    

    
    OPTIONS(
      description="""Flattened subscriber records from the raw snapshot. One row per subscriber per snapshot date. No deduplication \u2014 all snapshots retained for SCD2 tracking in the intermediate layer.\n"""
    )
    as (
      
    select snapshot_date, publication, subscription_id, user_id, is_subscribed, is_comp, is_gift, is_free_trial, subscription_interval, activity_rating, subscription_created_at, first_payment_at, subscription_expires_at, unsubscribed_at, total_count, email, stripe_plan_name, paid_attribution, free_attribution
    from (
        

with source as (
    select
        snapshot_date,
        publication,
        sub.subscription_id          as subscription_id,
        sub.user_id                  as user_id,
        sub.is_subscribed            as is_subscribed,
        sub.is_comp                  as is_comp,
        sub.is_gift                  as is_gift,
        sub.is_free_trial            as is_free_trial,
        sub.subscription_interval    as subscription_interval,
        sub.activity_rating          as activity_rating,
        sub.subscription_created_at  as subscription_created_at,
        sub.first_payment_at         as first_payment_at,
        sub.subscription_expires_at  as subscription_expires_at,
        sub.unsubscribed_at          as unsubscribed_at,
        sub.total_count              as total_count,
        sub.user_email_address       as email,
        sub.stripe_plan_name         as stripe_plan_name,
        sub.paid_attribution         as paid_attribution,
        sub.free_attribution         as free_attribution

    from `data-platform-455517`.`raw_landing`.`substack___subscribers_snapshot`
)

select * from source
    ) as model_subq
    );
  