{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_tasks',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'tasks', 'current'],
  )
}}

with raw_source as (
  select `id`, `created_by`, `updated_by`, `created_at`, `updated_at`, `responsible_user_id`, `group_id`, `entity_id`, `entity_type`, `is_completed`, `task_type_id`, `text`, `duration`, `complete_till`, `account_id`, `result_text`, `raw_data`
  from {{ bronze_parquet('crm', 'tasks', '`id` Nullable(UInt64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` String, `updated_at` String, `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `entity_id` Nullable(UInt64), `entity_type` Nullable(String), `is_completed` Nullable(String), `task_type_id` Nullable(UInt64), `text` Nullable(String), `duration` Nullable(Int64), `complete_till` String, `account_id` Nullable(UInt64), `result_text` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
  CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
  CAST(`group_id`, 'Nullable(UInt64)') AS `group_id`,
  CAST(`entity_id`, 'Nullable(UInt64)') AS `entity_id`,
  CAST(`entity_type`, 'Nullable(String)') AS `entity_type`,
  CAST(`is_completed`, 'Nullable(String)') AS `is_completed`,
  CAST(`task_type_id`, 'Nullable(UInt64)') AS `task_type_id`,
  CAST(`text`, 'Nullable(String)') AS `text`,
  CAST(`duration`, 'Nullable(Int64)') AS `duration`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`complete_till`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`complete_till`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `complete_till`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST(`result_text`, 'Nullable(String)') AS `result_text`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
