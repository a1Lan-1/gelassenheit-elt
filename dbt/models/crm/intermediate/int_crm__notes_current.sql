{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_notes',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'notes', 'current'],
  )
}}

with raw_source as (
  select `id`, `entity_id`, `entity_type`, `note_type`, `text`, `created_by`, `updated_by`, `created_at`, `updated_at`, `responsible_user_id`, `group_id`, `account_id`, `params`, `raw_data`
  from {{ bronze_parquet('crm', 'notes', '`id` Nullable(UInt64), `entity_id` Nullable(UInt64), `entity_type` Nullable(String), `note_type` Nullable(String), `text` Nullable(String), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` String, `updated_at` String, `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `account_id` Nullable(UInt64), `params` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`entity_id`, 'Nullable(UInt64)') AS `entity_id`,
  CAST(`entity_type`, 'Nullable(String)') AS `entity_type`,
  CAST(`note_type`, 'Nullable(String)') AS `note_type`,
  CAST(`text`, 'Nullable(String)') AS `text`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
  CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
  CAST(`group_id`, 'Nullable(UInt64)') AS `group_id`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`params`)), '')") }}, 'Nullable(JSON)') AS `params`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
