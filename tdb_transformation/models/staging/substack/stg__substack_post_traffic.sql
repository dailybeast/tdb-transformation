{{ config(materialized='table') }}

with source as (
    select
        post_id,
        snapshot_date,
        PARSE_JSON(data) as data
    from {{ source('raw_landing', 'substack___post_traffic') }}
),

pivoted as (
    select
        b.post_id,
        snapshot_date,

        -- Referrer views by source category
        sum(case when JSON_VALUE(r, '$.source_category') = 'Email'          then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_email,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Direct'         then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_direct,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Substack'       then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_substack,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Search'         then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_search,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Social'         then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_social,
        sum(case when JSON_VALUE(r, '$.source_category') = 'News'           then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_news,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Other External' then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_other_external,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Other Internal' then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_other_internal,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Other'          then SAFE_CAST(JSON_VALUE(r, '$.views') AS INT64) end) as referrer_views_other,

        -- Paid subscriber views by source category
        sum(case when JSON_VALUE(r, '$.source_category') = 'Email'          then SAFE_CAST(JSON_VALUE(r, '$.paid_subscriber_views') AS INT64) end) as paid_views_email,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Direct'         then SAFE_CAST(JSON_VALUE(r, '$.paid_subscriber_views') AS INT64) end) as paid_views_direct,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Substack'       then SAFE_CAST(JSON_VALUE(r, '$.paid_subscriber_views') AS INT64) end) as paid_views_substack,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Search'         then SAFE_CAST(JSON_VALUE(r, '$.paid_subscriber_views') AS INT64) end) as paid_views_search,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Social'         then SAFE_CAST(JSON_VALUE(r, '$.paid_subscriber_views') AS INT64) end) as paid_views_social,

        -- Free subscriber views by source category
        sum(case when JSON_VALUE(r, '$.source_category') = 'Email'          then SAFE_CAST(JSON_VALUE(r, '$.free_subscriber_views') AS INT64) end) as free_views_email,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Direct'         then SAFE_CAST(JSON_VALUE(r, '$.free_subscriber_views') AS INT64) end) as free_views_direct,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Substack'       then SAFE_CAST(JSON_VALUE(r, '$.free_subscriber_views') AS INT64) end) as free_views_substack,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Search'         then SAFE_CAST(JSON_VALUE(r, '$.free_subscriber_views') AS INT64) end) as free_views_search,
        sum(case when JSON_VALUE(r, '$.source_category') = 'Social'         then SAFE_CAST(JSON_VALUE(r, '$.free_subscriber_views') AS INT64) end) as free_views_social,

        -- Device views
        sum(case when JSON_VALUE(d, '$.device_type') = 'Email'        then SAFE_CAST(JSON_VALUE(d, '$.views') AS INT64) end) as device_views_email,
        sum(case when JSON_VALUE(d, '$.device_type') = 'Desktop Web'  then SAFE_CAST(JSON_VALUE(d, '$.views') AS INT64) end) as device_views_desktop_web,
        sum(case when JSON_VALUE(d, '$.device_type') = 'Mobile Web'   then SAFE_CAST(JSON_VALUE(d, '$.views') AS INT64) end) as device_views_mobile_web,
        sum(case when JSON_VALUE(d, '$.device_type') = 'Substack App' then SAFE_CAST(JSON_VALUE(d, '$.views') AS INT64) end) as device_views_substack_app,

        -- Category views
        sum(case when JSON_VALUE(c, '$.category_type') = 'Email'          then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_email,
        sum(case when JSON_VALUE(c, '$.category_type') = 'Direct'         then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_direct,
        sum(case when JSON_VALUE(c, '$.category_type') = 'Substack'       then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_substack,
        sum(case when JSON_VALUE(c, '$.category_type') = 'Search'         then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_search,
        sum(case when JSON_VALUE(c, '$.category_type') = 'Social'         then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_social,
        sum(case when JSON_VALUE(c, '$.category_type') = 'Other External' then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_other_external,
        sum(case when JSON_VALUE(c, '$.category_type') = 'Other'          then SAFE_CAST(JSON_VALUE(c, '$.category_views') AS INT64) end) as category_views_other

    from source as b,
        unnest(JSON_QUERY_ARRAY(b.data, '$.referrers'))  as r,
        unnest(JSON_QUERY_ARRAY(b.data, '$.devices'))    as d,
        unnest(JSON_QUERY_ARRAY(b.data, '$.categories')) as c
    group by post_id, snapshot_date
)

select * from pivoted
