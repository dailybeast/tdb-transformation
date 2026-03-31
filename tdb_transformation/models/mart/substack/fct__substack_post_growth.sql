{{ config(materialized='table') }}

select * from {{ ref('int__substack_post_growth') }}
