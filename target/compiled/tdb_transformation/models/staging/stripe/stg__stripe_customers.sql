

with source as (
    select
        id                      as customer_id,
        email,
        name,
        created                 as customer_created_at,
        delinquent
    from `ai-mvp-392019`.`stripe`.`customer`
)

select * from source