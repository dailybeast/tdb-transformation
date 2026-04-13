

with charges as (
    select *
    from `data-platform-455517`.`stripe`.`stg__stripe_charges`
    where invoice_id is not null
    ), 

    invoices as (
        select
            invoice_id,
            subscription_id,
        from `data-platform-455517`.`stripe`.`stg__stripe_invoices`
        where subscription_id is not null
    ),
    --comparing substack export subscription interval to stripe
    substack_subs as (
        select
            email,
            subscription_interval as ss_subscription_interval
        from `data-platform-455517`.`substack`.`stg__substack_subscribers`
        qualify row_number() over (partition by email order by snapshot_date desc) = 1
    ),


    subscriptions as (
        select
            subscription_id,
            customer_id,
            email,
            status
        from `data-platform-455517`.`stripe`.`int__stripe_substack_subscriptions`
    ),

    base as (
        select 
            ch.charge_id,
            ch.charged_at,
            ch.charge_currency,
            ch.exchange_rate,
            ch.settled_amount_usd,
            ch.net_amount_usd,
            inv.subscription_id,
            sub.customer_id,
            sub.email,
            case
                when stack_subs.ss_subscription_interval = 'annual'  then 'annual'
                when stack_subs.ss_subscription_interval = 'monthly' then 'monthly'
                when ch.settled_amount_usd >= 50                     then 'annual'
                else 'monthly'
            end  as billing_interval,
            stack_subs.ss_subscription_interval,
            sub.status
        from charges ch
        inner join invoices inv 
            on inv.invoice_id = ch.invoice_id
        inner join subscriptions sub
            on sub.subscription_id = inv.subscription_id
        left join substack_subs as stack_subs
            on stack_subs.email = sub.email
    ),

    finance_months as (
        select
            *,
            case when extract(day from date(charged_at)) >= 16
                then date_trunc(date_add(date(charged_at), interval 1 month), month)
                else date_trunc(date(charged_at), month)
            end as finance_month_date
        from base
    ),

    classified as (
        select  
            *, 
            format_date('%B %Y', finance_month_date) as reporting_month,
            date_add(date_sub(finance_month_date, interval 1 month), interval 15 day) as reporting_month_start,
            date_add(finance_month_date, interval 14 day) as reporting_month_end
        from finance_months
    )

    select 
        charge_id,
        subscription_id,
        customer_id,
        email,
        billing_interval,
        ss_subscription_interval,
        charged_at,
        charge_currency,
        exchange_rate,
        settled_amount_usd,
        net_amount_usd,
        format_date('%B %Y', date_add(finance_month_date, interval month_offset month)) as reporting_month,
        case when billing_interval = 'annual'
            then round(net_amount_usd / 12.0, 2)
        else net_amount_usd
        end as recognized_revenue_usd,
        month_offset
    from classified 
    cross join unnest(generate_array(0,
        case when billing_interval = 'annual' then 11 else 0 end)) as month_offset