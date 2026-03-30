{{ config(materialized='table') }}

with
source as (
    select
        post_id,
        snapshot_date,
        data
    from {{ source('raw_landing', 'substack_royalist___post_overview') }}
),

parsed as (
    select
        post_id,
        snapshot_date,

        -- Post metadata
        data.posts[0].id                                            as post_id_check,
        data.posts[0].uuid                                          as post_uuid,
        data.posts[0].title                                         as title,
        data.posts[0].slug                                          as slug,
        data.posts[0].type                                          as post_type,
        data.posts[0].audience                                      as audience,
        data.posts[0].meter_type                                    as meter_type,
        data.posts[0].is_published                                  as is_published,
        data.posts[0].should_send_email                             as should_send_email,
        data.posts[0].write_comment_permissions                     as write_comment_permissions,
        data.posts[0].publication_id                                as publication_id,
        data.posts[0].post_date                                     as post_date,
        data.posts[0].email_sent_at                                 as email_sent_at,
        data.posts[0].draft_created_at                              as draft_created_at,
        data.posts[0].draft_updated_at                              as draft_updated_at,
        data.posts[0].cover_image                                   as cover_image_url,

        -- Author
        data.posts[0].bylines[0].name                               as author_name,
        data.posts[0].bylines[0].handle                             as author_handle,

        -- Engagement counts
        data.posts[0].comment_count                                 as comment_count,
        data.posts[0].reaction_count                                as reaction_count,
        data.posts[0].child_comment_count                           as child_comment_count,

        -- ----------------------------------------------------------------
        -- ACTUALS
        -- ----------------------------------------------------------------

        -- Email delivery
        data.posts[0].stats.queued                                  as queued,
        data.posts[0].stats.sent                                    as sent,
        data.posts[0].stats.delivered                               as delivered,
        data.posts[0].stats.dropped                                 as dropped,
        data.posts[0].stats.opened                                  as opened,
        data.posts[0].stats.opens                                   as opens,
        data.posts[0].stats.clicked                                 as clicked,
        data.posts[0].stats.clicks                                  as clicks,

        -- Rates
        data.posts[0].stats.open_rate                               as open_rate,
        data.posts[0].stats.click_through_rate                      as click_through_rate,
        data.posts[0].stats.engagement_rate                         as engagement_rate,

        -- Views & reads
        data.posts[0].stats.views                                   as views,
        data.posts[0].stats.engaged                                 as engaged,
        data.posts[0].stats.subscribers_finished_post               as subscribers_finished_post,
        data.posts[0].stats.restacks                                as restacks,

        -- Subscriptions driven
        data.posts[0].stats.subscribes                              as subscribes,
        data.posts[0].stats.monthly_subscribes                      as monthly_subscribes,
        data.posts[0].stats.annual_subscribes                       as annual_subscribes,
        data.posts[0].stats.free_to_paid_upgrades                   as free_to_paid_upgrades,
        data.posts[0].stats.signups                                 as signups,
        data.posts[0].stats.likes                                   as likes,
        data.posts[0].stats.shares                                  as shares,
        data.posts[0].stats.estimated_value                         as estimated_value,

        -- Churn signals
        data.posts[0].stats.unsubscribes                            as unsubscribes,
        data.posts[0].stats.unsubscribes_within_1_day               as unsubscribes_within_1_day,
        data.posts[0].stats.disables_within_1_day                   as disables_within_1_day,
        data.posts[0].stats.subscriptions_within_1_day              as subscriptions_within_1_day,
        data.posts[0].stats.signups_within_1_day                    as signups_within_1_day,

        -- Referrer totals
        data.posts[0].stats.referrers.total_views                   as referrer_total_views,

        -- ----------------------------------------------------------------
        -- PUBLICATION AVERAGES (recent comparable Royalist posts)
        -- ----------------------------------------------------------------

        data.posts[0].stats.comps.n_comp_posts                      as pub_avg_n_comp_posts,
        data.posts[0].stats.comps.avg_queued                        as pub_avg_queued,
        data.posts[0].stats.comps.avg_sent                          as pub_avg_sent,
        data.posts[0].stats.comps.avg_delivered                     as pub_avg_delivered,
        data.posts[0].stats.comps.avg_dropped                       as pub_avg_dropped,
        data.posts[0].stats.comps.avg_opened                        as pub_avg_opened,
        data.posts[0].stats.comps.avg_opens                         as pub_avg_opens,
        data.posts[0].stats.comps.avg_clicked                       as pub_avg_clicked,
        data.posts[0].stats.comps.avg_clicks                        as pub_avg_clicks,
        data.posts[0].stats.comps.avg_open_rate                     as pub_avg_open_rate,
        data.posts[0].stats.comps.avg_click_through_rate            as pub_avg_click_through_rate,
        data.posts[0].stats.comps.avg_engagement_rate               as pub_avg_engagement_rate,
        data.posts[0].stats.comps.avg_views                         as pub_avg_views,
        data.posts[0].stats.comps.avg_unique_engagements            as pub_avg_unique_engagements,
        data.posts[0].stats.comps.avg_unique_opens_day7             as pub_avg_unique_opens_day7,
        data.posts[0].stats.comps.avg_unique_opens_day28            as pub_avg_unique_opens_day28,
        data.posts[0].stats.comps.avg_likes                         as pub_avg_likes,
        data.posts[0].stats.comps.avg_comments                      as pub_avg_comments,
        data.posts[0].stats.comps.avg_shares                        as pub_avg_shares,
        data.posts[0].stats.comps.avg_subscribes                    as pub_avg_subscribes,
        data.posts[0].stats.comps.avg_monthly_subscribes            as pub_avg_monthly_subscribes,
        data.posts[0].stats.comps.avg_annual_subscribes             as pub_avg_annual_subscribes,
        data.posts[0].stats.comps.avg_unsubscribes                  as pub_avg_unsubscribes,
        data.posts[0].stats.comps.avg_unsubscribes_within_1_day     as pub_avg_unsubscribes_within_1_day,
        data.posts[0].stats.comps.avg_disables_within_1_day         as pub_avg_disables_within_1_day,
        data.posts[0].stats.comps.avg_subscriptions_within_1_day    as pub_avg_subscriptions_within_1_day,
        data.posts[0].stats.comps.avg_signups_within_1_day          as pub_avg_signups_within_1_day,
        data.posts[0].stats.comps.avg_signups                       as pub_avg_signups,
        data.posts[0].stats.comps.avg_estimated_value               as pub_avg_estimated_value,

        data.posts[0].stats.data_updated_at                         as stats_updated_at

    from source
)

select * from parsed