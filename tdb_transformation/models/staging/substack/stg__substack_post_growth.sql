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
        data.subscribes.totals.subscribes           as subscribes,
        data.subscribes.totals.monthly_subscribes   as monthly_subscribes,
        data.subscribes.totals.annual_subscribes    as annual_subscribes,
        data.subscribes.totals.free_trials          as free_trials,
        data.subscribes.totals.founding_subscribes  as founding_subscribes,

        -- Signup and unsubscribe totals
        data.signups.total                          as signups,
        data.unsubscribes.total                     as unsubscribes

    from source
)

select * from parsed
