{{ config(materialized='table') }}

with

source as (
    select * from {{ ref('stg__substack_post_overview') }}
),

-- Some fields (e.g. engaged, likes, restacks) go null in newer snapshots when
-- Substack hasn't re-computed detailed stats yet (stats_updated_at is null).
-- Pre-compute last non-null values so deduplication always keeps good data.
-- Note: BigQuery does not allow analytic functions inside SELECT * REPLACE,
-- so these are computed as separate columns and referenced explicitly below.
with_last_known as (
    select
        *,
        last_value(queued                    ignore nulls) over (partition by post_id order by snapshot_date) as queued_last,
        last_value(dropped                   ignore nulls) over (partition by post_id order by snapshot_date) as dropped_last,
        last_value(engaged                   ignore nulls) over (partition by post_id order by snapshot_date) as engaged_last,
        last_value(subscribers_finished_post ignore nulls) over (partition by post_id order by snapshot_date) as subscribers_finished_post_last,
        last_value(restacks                  ignore nulls) over (partition by post_id order by snapshot_date) as restacks_last,
        last_value(monthly_subscribes        ignore nulls) over (partition by post_id order by snapshot_date) as monthly_subscribes_last,
        last_value(annual_subscribes         ignore nulls) over (partition by post_id order by snapshot_date) as annual_subscribes_last,
        last_value(free_to_paid_upgrades     ignore nulls) over (partition by post_id order by snapshot_date) as free_to_paid_upgrades_last,
        last_value(unsubscribes              ignore nulls) over (partition by post_id order by snapshot_date) as unsubscribes_last,
        last_value(likes                     ignore nulls) over (partition by post_id order by snapshot_date) as likes_last,
        last_value(timestamp_millis(stats_updated_at) ignore nulls) over (partition by post_id order by snapshot_date) as stats_updated_at_last
    from source
),

deduped as (
    select
        post_id,
        snapshot_date,

        -- Post metadata
        post_id_check,
        post_uuid,
        title,
        slug,
        post_type,
        audience,
        meter_type,
        is_published,
        should_send_email,
        write_comment_permissions,
        publication_id,
        post_date,
        email_sent_at,
        draft_created_at,
        draft_updated_at,
        cover_image_url,

        -- Author
        author_name,
        author_handle,

        -- Engagement counts
        comment_count,
        reaction_count,
        child_comment_count,

        -- Email delivery
        queued_last                    as queued,
        sent,
        delivered,
        dropped_last                   as dropped,
        opened,
        opens,
        clicked,
        clicks,

        -- Rates
        open_rate,
        click_through_rate,
        engagement_rate,

        -- Views & reads
        views,
        engaged_last                   as engaged,
        subscribers_finished_post_last as subscribers_finished_post,
        restacks_last                  as restacks,

        -- Subscriptions driven
        subscribes,
        monthly_subscribes_last        as monthly_subscribes,
        annual_subscribes_last         as annual_subscribes,
        free_to_paid_upgrades_last     as free_to_paid_upgrades,
        signups,
        likes_last                     as likes,
        shares,
        estimated_value,

        -- Churn signals
        unsubscribes_last              as unsubscribes,
        unsubscribes_within_1_day,
        disables_within_1_day,
        subscriptions_within_1_day,
        signups_within_1_day,

        -- Referrer totals
        referrer_total_views,

        -- Publication averages
        pub_avg_n_comp_posts,
        pub_avg_queued,
        pub_avg_sent,
        pub_avg_delivered,
        pub_avg_dropped,
        pub_avg_opened,
        pub_avg_opens,
        pub_avg_clicked,
        pub_avg_clicks,
        pub_avg_open_rate,
        pub_avg_click_through_rate,
        pub_avg_engagement_rate,
        pub_avg_views,
        pub_avg_unique_engagements,
        pub_avg_unique_opens_day7,
        pub_avg_unique_opens_day28,
        pub_avg_likes,
        pub_avg_comments,
        pub_avg_shares,
        pub_avg_subscribes,
        pub_avg_monthly_subscribes,
        pub_avg_annual_subscribes,
        pub_avg_unsubscribes,
        pub_avg_unsubscribes_within_1_day,
        pub_avg_disables_within_1_day,
        pub_avg_subscriptions_within_1_day,
        pub_avg_signups_within_1_day,
        pub_avg_signups,
        pub_avg_estimated_value,

        stats_updated_at_last          as stats_updated_at

    from with_last_known
    qualify row_number() over (partition by post_id order by snapshot_date desc) = 1
)

select * from deduped
