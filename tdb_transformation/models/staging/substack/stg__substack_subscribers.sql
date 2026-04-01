{{ config(materialized='table') }}

with source as (
    select
        snapshot_date,
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
        sub.total_count              as total_count
    from {{ source('raw_landing', 'substack___subscribers_snapshot') }}
)

select * from source
