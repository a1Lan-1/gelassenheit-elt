{{
  config(
    materialized='table',
    alias='stg_companies',
    engine=ch_engine_merge_tree(),
    order_by='(id)',
    settings={'allow_nullable_key': 1},
    tags=['crm', 'companies', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('crm', 'companies', '`id` Nullable(UInt64), `name` Nullable(String), `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` Nullable(DateTime64(3)), `updated_at` Nullable(DateTime64(3)), `closest_task_at` Nullable(DateTime64(3)), `is_deleted` Nullable(Bool), `custom_fields_values` Nullable(String), `inn` Nullable(String), `account_id` Nullable(UInt64), `tags` Nullable(String), `customers` Nullable(String), `leads` Nullable(String), `catalog_elements` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
  CAST(`group_id`, 'Nullable(UInt64)') AS `group_id`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(`created_at`), NULL, `created_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(`updated_at`), NULL, `updated_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `updated_at`,
  CAST(if(isNull(`closest_task_at`), NULL, `closest_task_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `closest_task_at`,
  CAST(`is_deleted`, 'Nullable(Bool)') AS `is_deleted`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
  CAST(`inn`, 'Nullable(String)') AS `inn`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`tags`)), '')") }}, 'Nullable(JSON)') AS `tags`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`customers`)), '')") }}, 'Nullable(JSON)') AS `customers`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`leads`)), '')") }}, 'Nullable(JSON)') AS `leads`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`catalog_elements`)), '')") }}, 'Nullable(JSON)') AS `catalog_elements`
from raw_source
