{{ config(materialized='table') }}

with source as (
    select
        date(snapshot_date)                                     as snapshot_date,
        snapshot_date                                           as snapshot_ts,
        concat('https://', publication, '.substack.com')        as source_uri,
        publication,
        sub.subscription_id                                     as subscription_id,
        sub.user_id                                             as user_id,
        sub.user_email_address                                  as email,
        sub.user_name                                           as name,
        sub.subscriber                                          as type,
        sub.stripe_plan_name                                    as stripe_plan,
        sub.is_subscribed                                       as is_subscribed,
        sub.is_comp                                             as is_comp,
        sub.is_gift                                             as is_gift,
        sub.is_free_trial                                       as is_free_trial,
        sub.subscription_interval                               as subscription_interval,
        sub.subscription_created_at                             as start_date,
        sub.first_payment_at                                    as first_paid_date,
        sub.subscription_expires_at                             as expiration_date,
        sub.unsubscribed_at                                     as cancel_date,
        round(sub.total_revenue_generated / 100.0, 2)           as revenue,
        sub.country                                             as country,
        sub.paid_attribution                                    as paid_source,
        sub.free_attribution                                    as free_source,
        sub.activity_rating                                     as activity,
        sub.total_count                                         as total_count
    from {{ source('raw_landing', 'substack___subscribers_snapshot') }}
)

select * from source
