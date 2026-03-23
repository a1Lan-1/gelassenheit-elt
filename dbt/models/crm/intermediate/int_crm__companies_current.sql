{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_companies',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'companies', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `responsible_user_id`, `group_id`, `created_by`, `updated_by`, `created_at`, `updated_at`, `closest_task_at`, `is_deleted`, `custom_fields_values`, `inn`, `account_id`, `tags`, `customers`, `leads`, `catalog_elements`
  from {{ bronze_parquet('crm', 'companies', '`id` Nullable(UInt64), `name` Nullable(String), `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` String, `updated_at` String, `closest_task_at` String, `is_deleted` Nullable(String), `custom_fields_values` Nullable(String), `inn` Nullable(String), `account_id` Nullable(UInt64), `tags` Nullable(String), `customers` Nullable(String), `leads` Nullable(String), `catalog_elements` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
  CAST(`group_id`, 'Nullable(UInt64)') AS `group_id`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closest_task_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closest_task_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `closest_task_at`,
  CAST(multiIf(lower(trimBoth(toString(`is_deleted`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`is_deleted`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`is_deleted`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`is_deleted`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `is_deleted`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
  CAST(`inn`, 'Nullable(String)') AS `inn`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`tags`)), '')") }}, 'Nullable(JSON)') AS `tags`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`customers`)), '')") }}, 'Nullable(JSON)') AS `customers`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`leads`)), '')") }}, 'Nullable(JSON)') AS `leads`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`catalog_elements`)), '')") }}, 'Nullable(JSON)') AS `catalog_elements`
from raw_source