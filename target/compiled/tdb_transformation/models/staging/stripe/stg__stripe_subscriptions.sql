

with source as (
    select
        id                      as subscription_id,
        customer_id,
        status,
        current_period_start,
        current_period_end,
        cancel_at_period_end,
        cancel_at,
        canceled_at,
        ended_at,
        created                 as subscription_created_at,
        start_date,
        trial_start,
        trial_end,
        _fivetran_active,
        _fivetran_start,
        _fivetran_end
    from `ai-mvp-392019`.`stripe`.`subscription_history`
)

select * from source