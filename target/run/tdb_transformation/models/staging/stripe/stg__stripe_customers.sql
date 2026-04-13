
  
    

    create or replace table `data-platform-455517`.`stripe`.`stg__stripe_customers`
      
    
    

    
    OPTIONS(
      description="""One row per Stripe customer. Email is the join key to Substack subscriber data.\n"""
    )
    as (
      

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
    );
  