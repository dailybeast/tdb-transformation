

with base as (
    select * 
    from `data-platform-455517`.`substack`.`int__substack_subscriber_daily`
    
    where date(snapshot_date) >= date_sub(current_date(), interval 3 day)
    
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
                    when subscription_expires_at > snapshot_date then 'Active'
                    else 'Expired'
                end
            when first_payment_at is null and not coalesce(is_comp, false) then 'Non-paid'
            when unsubscribed_at is not null then
                case
                    when subscription_expires_at > snapshot_date then 'Cancelled but Active'
                    else 'Expired'
                end
            when subscription_expires_at is null         then 'Expired'
            when subscription_expires_at > snapshot_date then 'Active'
            else 'Expired'
        end as status_bucket

    from base
)

select
    *,
    status_bucket in ('Active', 'Cancelled but Active') as is_active_paid
from buckets