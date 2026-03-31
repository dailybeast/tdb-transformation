{{ config(materialized='table') }}

with

source as (
    select * from {{ ref('stg__substack_post_growth') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by post_id order by snapshot_date desc) = 1
)

select * from deduped
