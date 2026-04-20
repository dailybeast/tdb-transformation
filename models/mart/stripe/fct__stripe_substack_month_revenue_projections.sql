{{ config(materialized='table') }}

with

current_period as (
    select
        case
            when extract(day from current_date()) >= 16
                then date_trunc(current_date(), month)
            else date_sub(date_trunc(current_date(), month), interval 1 month)
        end                                               as finance_month_date
),

reporting_window as (
    select
        format_date('%B %Y', finance_month_date)          as reporting_month,
        date_add(finance_month_date, interval 15 day)     as period_start,
        date_add(
            date_add(finance_month_date, interval 1 month),
            interval 14 day
        )                                                 as period_end,
        date_diff(
            date_add(date_add(finance_month_date, interval 1 month), interval 14 day),
            date_add(finance_month_date, interval 15 day),
            day
        ) + 1                                             as total_days,
        date_diff(
            current_date(),
            date_add(finance_month_date, interval 15 day),
            day
        ) + 1                                             as elapsed_days
    from current_period
),

actuals_base as (
    select
        format_date('%B %Y', reporting_month_start)       as reporting_month,
        reporting_month_start,
        reporting_month_end,
        billing_interval,
        recognized_revenue_usd                            as revenue,
        subscriber_count,
        round(safe_divide(recognized_revenue_usd, subscriber_count), 2)
                                                          as avg_rev_per_sub,
        row_number() over (
            partition by billing_interval
            order by reporting_month_end
        )                                                 as rn
    from {{ ref('fct__stripe_substack_month_accrual') }}
    where revenue_type = 'subscriber'
        and reporting_month_end < current_date()
),

historical_starts as (
    select
        billing_interval,
        format_date('%B %Y',
            case
                when extract(day from date(subscription_created_at)) >= 16
                    then date_trunc(date(subscription_created_at), month)
                else date_sub(date_trunc(date(subscription_created_at), month), interval 1 month)
            end
        )                                                 as reporting_month,
        count(distinct subscription_id)                   as start_count
    from {{ ref('int__stripe_substack_subscriptions') }}
    where subscription_created_at is not null
        and (canceled_at is null or date(canceled_at) != date(subscription_created_at))
    group by 1, 2
),

historical_churn as (
    select
        billing_interval,
        reporting_month,
        count(distinct subscription_id)                   as churn_count
    from (
        select
            s.subscription_id,
            s.billing_interval,
            ca.reporting_month
        from {{ ref('int__stripe_substack_subscriptions') }} s
        join {{ ref('fct__stripe_substack_charge_accrual') }} ca
            on  ca.subscription_id = s.subscription_id
            and ca.revenue_type = 'subscriber'
            and ca.reporting_month_start <= date(s.canceled_at)
        where s.canceled_at is not null
            and date(s.canceled_at) != date(s.subscription_created_at)
        qualify row_number() over (
            partition by s.subscription_id
            order by ca.reporting_month_end desc
        ) = 1
    )
    group by 1, 2
),

starts_revenue as (
    select
        ca.billing_interval,
        ca.reporting_month                                 as reporting_month,
        round(sum(ca.recognized_revenue_usd), 2)          as starts_revenue
    from {{ ref('fct__stripe_substack_charge_accrual') }} ca
    join {{ ref('int__stripe_substack_subscriptions') }} s
        on  s.subscription_id = ca.subscription_id
        and ca.reporting_month_start = date_add(
            case
                when extract(day from date(s.subscription_created_at)) >= 16
                    then date_trunc(date(s.subscription_created_at), month)
                else date_sub(date_trunc(date(s.subscription_created_at), month), interval 1 month)
            end,
            interval 15 day
        )
    where ca.revenue_type = 'subscriber'
        and s.subscription_created_at is not null
        and (s.canceled_at is null or date(s.canceled_at) != date(s.subscription_created_at))
    group by 1, 2
),

churned_sub_last_accrual as (
    select
        s.subscription_id,
        s.billing_interval,
        ca.reporting_month                                as last_charge_reporting_month,
        ca.recognized_revenue_usd
    from {{ ref('int__stripe_substack_subscriptions') }} s
    join {{ ref('fct__stripe_substack_charge_accrual') }} ca
        on  ca.subscription_id = s.subscription_id
        and ca.revenue_type = 'subscriber'
        and ca.reporting_month_start <= date(s.canceled_at)
    where s.canceled_at is not null
        and date(s.canceled_at) != date(s.subscription_created_at)
    qualify row_number() over (
        partition by s.subscription_id
        order by ca.reporting_month_end desc
    ) = 1
),

churn_revenue as (
    select
        billing_interval,
        last_charge_reporting_month                       as reporting_month,
        round(sum(recognized_revenue_usd), 2)             as churn_revenue
    from churned_sub_last_accrual
    group by 1, 2
),

revenue_with_delta as (
    select
        rn,
        reporting_month,
        reporting_month_start,
        reporting_month_end,
        billing_interval,
        revenue,
        revenue - lag(revenue) over (
            partition by billing_interval
            order by reporting_month_end
        )                                                 as net_change
    from actuals_base
),

revenue_deltas as (
    select
        t.reporting_month                                 as target_month,
        t.reporting_month_start                          as target_start,
        t.reporting_month_end                             as target_end,
        t.billing_interval,
        'closed'                                          as row_type,
        l1.revenue                                        as prior_revenue,
        l1.net_change                                     as d1,
        l2.net_change                                     as d2,
        l3.net_change                                     as d3,
        l4.net_change                                     as d4,
        l5.net_change                                     as d5
    from actuals_base t
    left join revenue_with_delta l1 on l1.billing_interval = t.billing_interval and l1.rn = t.rn - 1
    left join revenue_with_delta l2 on l2.billing_interval = t.billing_interval and l2.rn = t.rn - 2
    left join revenue_with_delta l3 on l3.billing_interval = t.billing_interval and l3.rn = t.rn - 3
    left join revenue_with_delta l4 on l4.billing_interval = t.billing_interval and l4.rn = t.rn - 4
    left join revenue_with_delta l5 on l5.billing_interval = t.billing_interval and l5.rn = t.rn - 5
    where l5.net_change is not null

    union all

    select
        rw.reporting_month                                as target_month,
        rw.period_start                                   as target_start,
        rw.period_end                                     as target_end,
        t.billing_interval,
        'live'                                            as row_type,
        t.revenue                                         as prior_revenue,
        t.net_change                                      as d1,
        l2.net_change                                     as d2,
        l3.net_change                                     as d3,
        l4.net_change                                     as d4,
        l5.net_change                                     as d5
    from (
        select *, max(rn) over (partition by billing_interval) as max_rn
        from revenue_with_delta
    ) t
    cross join reporting_window rw
    left join revenue_with_delta l2 on l2.billing_interval = t.billing_interval and l2.rn = t.rn - 1
    left join revenue_with_delta l3 on l3.billing_interval = t.billing_interval and l3.rn = t.rn - 2
    left join revenue_with_delta l4 on l4.billing_interval = t.billing_interval and l4.rn = t.rn - 3
    left join revenue_with_delta l5 on l5.billing_interval = t.billing_interval and l5.rn = t.rn - 4
    where t.rn = t.max_rn
        and l5.net_change is not null
),

revenue_ewma as (
    select
        target_month,
        target_start,
        target_end,
        billing_interval,
        row_type,
        prior_revenue,
        round(
            (
                16 * case when d1 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d1 end +
                 8 * case when d2 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d2 end +
                 4 * case when d3 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d3 end +
                 2 * case when d4 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d4 end +
                 1 * case when d5 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d5 end
            ) / 31.0,
        2)                                                as ewma_delta,
        round(
            prior_revenue + (
                16 * case when d1 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d1 end +
                 8 * case when d2 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d2 end +
                 4 * case when d3 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d3 end +
                 2 * case when d4 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d4 end +
                 1 * case when d5 > 1.5 * ((d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0)
                        then (d1+d2+d3+d4+d5 - greatest(d1,d2,d3,d4,d5) - least(d1,d2,d3,d4,d5)) / 3.0
                        else d5 end
            ) / 31.0,
        2)                                                as projected_revenue
    from revenue_deltas
),

live_starts as (
    select
        billing_interval,
        count(distinct subscription_id)                   as starts_to_date
    from {{ ref('int__stripe_substack_subscriptions') }}
    cross join reporting_window rw
    where date(subscription_created_at)
        between rw.period_start and current_date()
        and (canceled_at is null or date(canceled_at) != date(subscription_created_at))
    group by 1
),

live_churn as (
    select
        billing_interval,
        count(distinct subscription_id)                   as churn_to_date
    from {{ ref('int__stripe_substack_subscriptions') }}
    cross join reporting_window rw
    where date(canceled_at)
        between rw.period_start and current_date()
        and date(canceled_at) != date(subscription_created_at)
    group by 1
),

live_revenue as (
    select
        billing_interval,
        round(sum(recognized_revenue_usd), 2)             as revenue_to_date
    from {{ ref('fct__stripe_substack_charge_accrual') }}
    cross join reporting_window rw
    where reporting_month_start = rw.period_start
        and revenue_type = 'subscriber'
    group by 1
)

select
    re.target_month                                       as reporting_month,
    re.target_start,
    re.target_end,
    re.billing_interval,
    re.row_type,
    case when re.row_type = 'live'
        then round(rw.elapsed_days / rw.total_days * 100, 1)
    end                                                   as pct_period_elapsed,
    round(re.prior_revenue, 2)                            as recurring_revenue,
    case when re.row_type = 'closed'
        then a.subscriber_count - hs.start_count
    end                                                   as recurring_subscribers,
    case
        when re.row_type = 'closed' then hs.start_count
        when re.row_type = 'live'   then ls.starts_to_date
    end                                                   as starts,
    case when re.row_type = 'live'
        then round(ls.starts_to_date / rw.elapsed_days * rw.total_days, 1)
    end                                                   as starts_prorated,
    case when re.row_type = 'closed'
        then sr.starts_revenue
    end                                                   as starts_revenue,
    case
        when re.row_type = 'closed' then hc.churn_count
        when re.row_type = 'live'   then lc.churn_to_date
    end                                                   as churn,
    case when re.row_type = 'live'
        then round(lc.churn_to_date / rw.elapsed_days * rw.total_days, 1)
    end                                                   as churn_prorated,
    case when re.row_type = 'closed'
        then cr.churn_revenue
    end                                                   as churn_revenue,
    case
        when re.row_type = 'closed' then round(a.revenue, 2)
        when re.row_type = 'live'   then lr.revenue_to_date
    end                                                   as actual_revenue,
    re.projected_revenue,
    case
        when re.row_type = 'closed'
            then round(safe_divide(re.projected_revenue - a.revenue, a.revenue) * 100, 1)
        when re.row_type = 'live'
            then round(safe_divide(lr.revenue_to_date, re.projected_revenue) * 100, 1)
    end                                                   as pct_of_projection_achieved
from revenue_ewma re
cross join reporting_window rw
left join actuals_base a
    on  a.reporting_month  = re.target_month
    and a.billing_interval = re.billing_interval
left join historical_starts hs
    on  hs.billing_interval = re.billing_interval
    and hs.reporting_month  = re.target_month
left join historical_churn hc
    on  hc.billing_interval = re.billing_interval
    and hc.reporting_month  = re.target_month
left join starts_revenue sr
    on  sr.billing_interval = re.billing_interval
    and sr.reporting_month  = re.target_month
left join churn_revenue cr
    on  cr.billing_interval = re.billing_interval
    and cr.reporting_month  = re.target_month
left join live_starts ls
    on  ls.billing_interval = re.billing_interval
left join live_churn lc
    on  lc.billing_interval = re.billing_interval
left join live_revenue lr
    on  lr.billing_interval = re.billing_interval
order by re.billing_interval, re.target_end
