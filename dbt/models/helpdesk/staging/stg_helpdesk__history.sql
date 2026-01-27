{{
  config(
    materialized='view',
    alias='stg_history',
    tags=['helpdesk', 'history', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `user_id`, `action_type`, `object_type`, `object_id`, `old_values`, `new_values`, `fields`, `created_at`, `updated_at`, `deleted_at`
  from {{ s3_parquet_run('helpdesk', 'history', '`id` String, `user_id` String, `action_type` Nullable(String), `object_type` Nullable(String), `object_id` String, `old_values` Nullable(String), `new_values` Nullable(String), `fields` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_id`)), '')), 'Nullable(UUID)') AS `user_id`,
  assumeNotNull(CAST(`action_type`, 'Nullable(String)')) AS `action_type`,
  assumeNotNull(CAST(`object_type`, 'Nullable(String)')) AS `object_type`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`object_id`)), '')), 'Nullable(UUID)')) AS `object_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`old_values`)), '')") }}, 'Nullable(JSON)') AS `old_values`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`new_values`)), '')") }}, 'Nullable(JSON)') AS `new_values`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`fields`)), '')") }}, 'Nullable(JSON)') AS `fields`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`
from raw_source
