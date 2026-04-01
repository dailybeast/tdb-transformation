{{ config(materialized='table') }}

with source as (
    select
        comment_id,
        post_id,
        parent_comment_id,
        body,
        snapshot_date
    from {{ source('raw_landing', 'substack___post_comments') }}
)

select * from source
