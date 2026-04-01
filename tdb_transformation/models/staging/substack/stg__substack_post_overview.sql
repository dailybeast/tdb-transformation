{{ config(materialized='table') }}

with
source as (
    select
        post_id,
        snapshot_date,
        PARSE_JSON(data) as data
    from {{ source('raw_landing', 'substack___post_overview') }}
),

parsed as (
    select
        post_id,
        snapshot_date,

        -- Post metadata
        SAFE.INT64(data.posts[0].id)                                             as post_id_check,
        SAFE.STRING(data.posts[0].uuid)                                          as post_uuid,
        SAFE.STRING(data.posts[0].title)                                         as title,
        SAFE.STRING(data.posts[0].slug)                                          as slug,
        SAFE.STRING(data.posts[0].type)                                          as post_type,
        SAFE.STRING(data.posts[0].audience)                                      as audience,
        SAFE.STRING(data.posts[0].meter_type)                                    as meter_type,
        SAFE.BOOL(data.posts[0].is_published)                                    as is_published,
        SAFE.BOOL(data.posts[0].should_send_email)                               as should_send_email,
        SAFE.STRING(data.posts[0].write_comment_permissions)                     as write_comment_permissions,
        SAFE.INT64(data.posts[0].publication_id)                                 as publication_id,
        TIMESTAMP(SAFE.STRING(data.posts[0].post_date))                          as post_date,
        TIMESTAMP(SAFE.STRING(data.posts[0].email_sent_at))                      as email_sent_at,
        TIMESTAMP(SAFE.STRING(data.posts[0].draft_created_at))                   as draft_created_at,
        TIMESTAMP(SAFE.STRING(data.posts[0].draft_updated_at))                   as draft_updated_at,
        SAFE.STRING(data.posts[0].cover_image)                                   as cover_image_url,

        -- Author
        SAFE.STRING(data.posts[0].bylines[0].name)                               as author_name,
        SAFE.STRING(data.posts[0].bylines[0].handle)                             as author_handle,

        -- Engagement counts
        SAFE.INT64(data.posts[0].comment_count)                                  as comment_count,
        SAFE.INT64(data.posts[0].reaction_count)                                 as reaction_count,
        SAFE.INT64(data.posts[0].child_comment_count)                            as child_comment_count,

        -- ----------------------------------------------------------------
        -- ACTUALS
        -- ----------------------------------------------------------------

        -- Email delivery
        SAFE.INT64(data.posts[0].stats.queued)                                   as queued,
        SAFE.INT64(data.posts[0].stats.sent)                                     as sent,
        SAFE.INT64(data.posts[0].stats.delivered)                                as delivered,
        SAFE.INT64(data.posts[0].stats.dropped)                                  as dropped,
        SAFE.INT64(data.posts[0].stats.opened)                                   as opened,
        SAFE.INT64(data.posts[0].stats.opens)                                    as opens,
        SAFE.INT64(data.posts[0].stats.clicked)                                  as clicked,
        SAFE.INT64(data.posts[0].stats.clicks)                                   as clicks,

        -- Rates
        SAFE.FLOAT64(data.posts[0].stats.open_rate)                              as open_rate,
        SAFE.FLOAT64(data.posts[0].stats.click_through_rate)                     as click_through_rate,
        SAFE.FLOAT64(data.posts[0].stats.engagement_rate)                        as engagement_rate,

        -- Views & reads
        SAFE.INT64(data.posts[0].stats.views)                                    as views,
        SAFE.INT64(data.posts[0].stats.engaged)                                  as engaged,
        SAFE.INT64(data.posts[0].stats.subscribers_finished_post)                as subscribers_finished_post,
        SAFE.INT64(data.posts[0].stats.restacks)                                 as restacks,

        -- Subscriptions driven
        SAFE.INT64(data.posts[0].stats.subscribes)                               as subscribes,
        SAFE.INT64(data.posts[0].stats.monthly_subscribes)                       as monthly_subscribes,
        SAFE.INT64(data.posts[0].stats.annual_subscribes)                        as annual_subscribes,
        SAFE.INT64(data.posts[0].stats.free_to_paid_upgrades)                    as free_to_paid_upgrades,
        SAFE.INT64(data.posts[0].stats.signups)                                  as signups,
        SAFE.INT64(data.posts[0].stats.likes)                                    as likes,
        SAFE.INT64(data.posts[0].stats.shares)                                   as shares,
        SAFE.FLOAT64(data.posts[0].stats.estimated_value)                        as estimated_value,

        -- Churn signals
        SAFE.INT64(data.posts[0].stats.unsubscribes)                             as unsubscribes,
        SAFE.INT64(data.posts[0].stats.unsubscribes_within_1_day)                as unsubscribes_within_1_day,
        SAFE.INT64(data.posts[0].stats.disables_within_1_day)                    as disables_within_1_day,
        SAFE.INT64(data.posts[0].stats.subscriptions_within_1_day)               as subscriptions_within_1_day,
        SAFE.INT64(data.posts[0].stats.signups_within_1_day)                     as signups_within_1_day,

        -- Referrer totals
        SAFE.INT64(data.posts[0].stats.referrers.total_views)                    as referrer_total_views,

        -- ----------------------------------------------------------------
        -- PUBLICATION AVERAGES (recent comparable Royalist posts)
        -- ----------------------------------------------------------------

        SAFE.INT64(data.posts[0].stats.comps.n_comp_posts)                       as pub_avg_n_comp_posts,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_queued)                       as pub_avg_queued,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_sent)                         as pub_avg_sent,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_delivered)                    as pub_avg_delivered,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_dropped)                      as pub_avg_dropped,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_opened)                       as pub_avg_opened,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_opens)                        as pub_avg_opens,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_clicked)                      as pub_avg_clicked,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_clicks)                       as pub_avg_clicks,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_open_rate)                    as pub_avg_open_rate,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_click_through_rate)           as pub_avg_click_through_rate,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_engagement_rate)              as pub_avg_engagement_rate,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_views)                        as pub_avg_views,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_unique_engagements)           as pub_avg_unique_engagements,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_unique_opens_day7)            as pub_avg_unique_opens_day7,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_unique_opens_day28)           as pub_avg_unique_opens_day28,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_likes)                        as pub_avg_likes,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_comments)                     as pub_avg_comments,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_shares)                       as pub_avg_shares,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_subscribes)                   as pub_avg_subscribes,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_monthly_subscribes)           as pub_avg_monthly_subscribes,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_annual_subscribes)            as pub_avg_annual_subscribes,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_unsubscribes)                 as pub_avg_unsubscribes,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_unsubscribes_within_1_day)    as pub_avg_unsubscribes_within_1_day,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_disables_within_1_day)        as pub_avg_disables_within_1_day,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_subscriptions_within_1_day)   as pub_avg_subscriptions_within_1_day,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_signups_within_1_day)         as pub_avg_signups_within_1_day,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_signups)                      as pub_avg_signups,
        SAFE.FLOAT64(data.posts[0].stats.comps.avg_estimated_value)              as pub_avg_estimated_value,

        SAFE.INT64(data.posts[0].stats.data_updated_at)                          as stats_updated_at

    from source
)

select * from parsed
