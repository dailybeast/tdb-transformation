{{ config(materialized='view') }}

select
    snapshot_date,
    publication,
    email,
    cast(null as string) as name,

    --royalist type definition
    -- case
    --     when publication = 'royalist' is_gift and billing_interval = 'monthly'  then 'monthly gift'
    --     when publication = 'royalist' is_gift and billing_interval = 'annual'   then 'yearly gift'
    --     when publication = 'royalist' subscription_interval = 'lifetime'        then 'royalist'
    --     when publication = 'royalist' is_comp                                   then 'comp'
    --     when publication = 'royalist' billing_interval = 'annual'               then 'yearly'
    --     when publication = 'royalist' billing_interval = 'monthly'              then 'monthly'
    --     else 'subscriber'
    -- end                                                 as type,
    is_gift,
    billing_interval,
    is_comp,
    subscription_interval,
    subscription_created_at                             as start_date,
    first_payment_at                                    as first_paid_date,
    unsubscribed_at                                     as cancel_date,
    subscription_expires_at                             as expiration_date,
    imputed_price_usd                                   as revenue

from {{ ref('fct__substack_subscriber_daily') }}
