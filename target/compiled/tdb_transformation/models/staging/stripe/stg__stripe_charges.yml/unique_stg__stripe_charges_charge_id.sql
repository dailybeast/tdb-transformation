
    
    

with dbt_test__target as (

  select charge_id as unique_field
  from `data-platform-455517`.`stripe`.`stg__stripe_charges`
  where charge_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


