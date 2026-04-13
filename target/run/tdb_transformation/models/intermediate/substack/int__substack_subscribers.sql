
  
    

    create or replace table `data-platform-455517`.`substack`.`int__substack_subscribers`
      
    
    

    
    OPTIONS(
      description="""Deduped subscriber records, one row per subscription_id (latest snapshot). Feeds the SCD2 snapshot for historical churn tracking.\n"""
    )
    as (
      

with

source as (
    select * from `data-platform-455517`.`substack`.`stg__substack_subscribers`
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by subscription_id order by snapshot_date desc) = 1
)

select * from deduped
    );
  