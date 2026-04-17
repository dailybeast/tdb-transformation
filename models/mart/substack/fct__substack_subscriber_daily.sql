{{ config(
    materialized='incremental',
    partition_by={
        'field': 'snapshot_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    incremental_strategy='insert_overwrite'
) }}

with base as (
    select *
    from {{ ref('int__substack_subscriber_daily') }}
    {% if is_incremental() %}
    where snapshot_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

buckets as (
    select
        *,

        case
            when is_gift and billing_interval = 'monthly'  then 'Monthly Gift'
            when is_gift and billing_interval = 'annual'   then 'Yearly Gift'
            when subscription_interval = 'lifetime'        then 'Royal Tier'
            when is_comp and billing_interval = 'annual'   then 'Yearly Subscriber'
            when is_comp and billing_interval = 'monthly'  then 'Monthly Subscriber'
            when is_comp                                   then 'Comp'
            when billing_interval = 'annual'               then 'Yearly Subscriber'
            when billing_interval = 'monthly'              then 'Monthly Subscriber'
            else 'Other'
        end as type_bucket,

        case
            when is_gift then
                case
                    when expiration_date > snapshot_date then 'Active'
                    else 'Expired'
                end
            when first_paid_date is null and not coalesce(is_comp, false) then 'Non-paid'
            when cancel_date is not null then
                case
                    when expiration_date > snapshot_date then 'Cancelled but Active'
                    else 'Expired'
                end
            when expiration_date is null         then 'Expired'
            when expiration_date > snapshot_date then 'Active'
            else 'Expired'
        end as status_bucket

    from base
)

select
    *,
    status_bucket in ('Active', 'Cancelled but Active') as is_active_paid
from buckets
