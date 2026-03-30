{{ config(materialized='table') }}

with

source as (
    select * from {{ ref('stg__substack_post_overview') }}
),

deduped as (
    select
        * replace (
            timestamp_millis(stats_updated_at) as stats_updated_at
        )
    from source
    qualify row_number() over (partition by post_id order by snapshot_date desc) = 1
)

select * from deduped
