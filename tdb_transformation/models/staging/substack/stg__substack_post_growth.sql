{{ config(materialized='table') }}

with source as (
    select
        post_id,
        snapshot_date,
        PARSE_JSON(data) as data
    from {{ source('raw_landing', 'substack___post_growth') }}
),

parsed as (
    select
        post_id,
        snapshot_date,

        -- Subscription totals
        CAST(data.subscribes.totals.subscribes          AS INT64) as subscribes,
        CAST(data.subscribes.totals.monthly_subscribes  AS INT64) as monthly_subscribes,
        CAST(data.subscribes.totals.annual_subscribes   AS INT64) as annual_subscribes,
        CAST(data.subscribes.totals.free_trials         AS INT64) as free_trials,
        CAST(data.subscribes.totals.founding_subscribes AS INT64) as founding_subscribes,

        -- Signup and unsubscribe totals
        CAST(data.signups.total                         AS INT64) as signups,
        CAST(data.unsubscribes.total                    AS INT64) as unsubscribes

    from source
)

select * from parsed
