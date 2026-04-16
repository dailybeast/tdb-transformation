{{ config(materialized='table') }}

with source as (
    select *
    from {{ ref('stg__stripe_charges') }}
    where description like 'Earnings from App Store subscriptions for%'
),
parsed as (
    select
        charge_id,
        charged_at,
        description,
        settled_amount_usd,
        net_amount_usd,
        charge_currency,
        regexp_extract(description, r'for (.+)$') as payout_month_label,
        parse_date(
            '%B %Y',
            regexp_extract(description, r'for (.+)$')
        ) as payout_month_date
    from source
)

    select
        charge_id,
        charged_at,
        description,
        settled_amount_usd,
        net_amount_usd,
        charge_currency,
        payout_month_label,
        format_date('%B %Y', payout_month_date) as reporting_month,
        date_add(date_sub(payout_month_date, interval 1 month), interval 15 day) as reporting_month_start,
        date_add(payout_month_date, interval 14 day) as reporting_month_end
    from parsed

),

finance_months as (
    select
        *,
        case
            when extract(day from date(charged_at)) >= 16
                then date_trunc(date(charged_at), month)
            else date_sub(date_trunc(date(charged_at), month), interval 1 month)
        end as finance_month_date
    from parsed
)

select
    charge_id,
    charged_at,
    description,
    settled_amount_usd,
    net_amount_usd,
    charge_currency,
    payout_month_label,
    format_date('%B %Y', finance_month_date)                                    as reporting_month,
    date_add(finance_month_date, interval 15 day)                              as reporting_month_start,
    date_add(date_add(finance_month_date, interval 1 month), interval 14 day)  as reporting_month_end
from finance_months
