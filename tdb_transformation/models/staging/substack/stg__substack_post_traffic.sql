{{ config(materialized='table') }}

with source as (
    select
        b.post_id,
        snapshot_date,

        -- Referrer views by source category
        sum(case when r.source_category = 'Email'          then r.views end) as referrer_views_email,
        sum(case when r.source_category = 'Direct'         then r.views end) as referrer_views_direct,
        sum(case when r.source_category = 'Substack'       then r.views end) as referrer_views_substack,
        sum(case when r.source_category = 'Search'         then r.views end) as referrer_views_search,
        sum(case when r.source_category = 'Social'         then r.views end) as referrer_views_social,
        sum(case when r.source_category = 'News'           then r.views end) as referrer_views_news,
        sum(case when r.source_category = 'Other External' then r.views end) as referrer_views_other_external,
        sum(case when r.source_category = 'Other Internal' then r.views end) as referrer_views_other_internal,
        sum(case when r.source_category = 'Other'          then r.views end) as referrer_views_other,

        -- Paid subscriber views by source category
        sum(case when r.source_category = 'Email'          then r.paid_subscriber_views end) as paid_views_email,
        sum(case when r.source_category = 'Direct'         then r.paid_subscriber_views end) as paid_views_direct,
        sum(case when r.source_category = 'Substack'       then r.paid_subscriber_views end) as paid_views_substack,
        sum(case when r.source_category = 'Search'         then r.paid_subscriber_views end) as paid_views_search,
        sum(case when r.source_category = 'Social'         then r.paid_subscriber_views end) as paid_views_social,

        -- Free subscriber views by source category
        sum(case when r.source_category = 'Email'          then r.free_subscriber_views end) as free_views_email,
        sum(case when r.source_category = 'Direct'         then r.free_subscriber_views end) as free_views_direct,
        sum(case when r.source_category = 'Substack'       then r.free_subscriber_views end) as free_views_substack,
        sum(case when r.source_category = 'Search'         then r.free_subscriber_views end) as free_views_search,
        sum(case when r.source_category = 'Social'         then r.free_subscriber_views end) as free_views_social,

        -- Device views
        sum(case when d.device_type = 'Email'        then d.views end) as device_views_email,
        sum(case when d.device_type = 'Desktop Web'  then d.views end) as device_views_desktop_web,
        sum(case when d.device_type = 'Mobile Web'   then d.views end) as device_views_mobile_web,
        sum(case when d.device_type = 'Substack App' then d.views end) as device_views_substack_app,

        -- Category views
        sum(case when c.category_type = 'Email'          then c.category_views end) as category_views_email,
        sum(case when c.category_type = 'Direct'         then c.category_views end) as category_views_direct,
        sum(case when c.category_type = 'Substack'       then c.category_views end) as category_views_substack,
        sum(case when c.category_type = 'Search'         then c.category_views end) as category_views_search,
        sum(case when c.category_type = 'Social'         then c.category_views end) as category_views_social,
        sum(case when c.category_type = 'Other External' then c.category_views end) as category_views_other_external,
        sum(case when c.category_type = 'Other'          then c.category_views end) as category_views_other

    from {{ source('raw_landing', 'substack_royalist___post_traffic') }} as b,
        unnest(data.referrers) as r,
        unnest(data.devices)   as d,
        unnest(data.categories) as c
    group by post_id, snapshot_date
)

select * from source
