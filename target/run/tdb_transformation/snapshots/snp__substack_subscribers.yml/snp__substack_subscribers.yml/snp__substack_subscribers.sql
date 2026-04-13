
      merge into `data-platform-455517`.`tdb_transformation_dev`.`snp__substack_subscribers` as DBT_INTERNAL_DEST
    using `data-platform-455517`.`tdb_transformation_dev`.`snp__substack_subscribers__dbt_tmp` as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     
       and DBT_INTERNAL_DEST.dbt_valid_to is null
     
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert (`snapshot_date`, `publication`, `subscription_id`, `user_id`, `is_subscribed`, `is_comp`, `is_gift`, `is_free_trial`, `subscription_interval`, `activity_rating`, `subscription_created_at`, `first_payment_at`, `subscription_expires_at`, `unsubscribed_at`, `total_count`, `email`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)
        values (`snapshot_date`, `publication`, `subscription_id`, `user_id`, `is_subscribed`, `is_comp`, `is_gift`, `is_free_trial`, `subscription_interval`, `activity_rating`, `subscription_created_at`, `first_payment_at`, `subscription_expires_at`, `unsubscribed_at`, `total_count`, `email`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`)


  