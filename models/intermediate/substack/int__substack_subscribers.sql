{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg__substack_subscribers') }}
),

mapped as (
    select
        snapshot_date,
        snapshot_ts,
        source_uri,
        email,
        name,
        coalesce(initcap(subscription_interval), 'Free')    as type,
        stripe_plan,
        cancel_date,
        start_date,
        expiration_date,
        first_paid_date,
        revenue,
        country,
        paid_source,
        free_source,
        activity,
        cast(null as string)                                as sections,

        -- retained for downstream enrichment
        publication,
        subscription_id,
        subscription_interval,
        is_subscribed,
        is_comp,
        is_gift,
        is_free_trial,
        total_count
    from source
),

deduped as (
    select *
    from mapped
    qualify row_number() over (
        partition by subscription_id order by snapshot_date desc
    ) = 1
)

select * from deduped