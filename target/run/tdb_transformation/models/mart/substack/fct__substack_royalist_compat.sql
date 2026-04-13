

  create or replace view `data-platform-455517`.`substack`.`fct__substack_royalist_compat`
  OPTIONS(
      description=""""""
    )
  as 

select
    snapshot_date,
    email,
    cast(null as string)                                as name,

    case
        when is_gift and billing_interval = 'monthly'  then 'monthly gift'
        when is_gift and billing_interval = 'annual'   then 'yearly gift'
        when subscription_interval = 'lifetime'        then 'royalist'
        when is_comp                                   then 'comp'
        when billing_interval = 'annual'               then 'yearly'
        when billing_interval = 'monthly'              then 'monthly'
        else 'subscriber'
    end                                                 as type,

    subscription_created_at                             as start_date,
    first_payment_at                                    as first_paid_date,
    unsubscribed_at                                     as cancel_date,
    subscription_expires_at                             as expiration_date,
    imputed_price_usd                                   as revenue

from `data-platform-455517`.`substack`.`fct__substack_subscriber_daily`;

