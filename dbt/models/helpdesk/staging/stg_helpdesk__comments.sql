{{
  config(
    materialized='view',
    alias='stg_comments',
    tags=['helpdesk', 'comments', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `foreign_id`, `table_name`, `text`, `user_id`, `created_at`, `updated_at`, `deleted_at`, `notify`, `person_id`
  from {{ s3_parquet_run('helpdesk', 'comments', '`id` String, `foreign_id` String, `table_name` Nullable(String), `text` Nullable(String), `user_id` String, `created_at` String, `updated_at` String, `deleted_at` String, `notify` Nullable(String), `person_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`foreign_id`)), '')), 'Nullable(UUID)') AS `foreign_id`,
  CAST(`table_name`, 'Nullable(String)') AS `table_name`,
  CAST(`text`, 'Nullable(String)') AS `text`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_id`)), '')), 'Nullable(UUID)') AS `user_id`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`notify`)), '')") }}, 'Nullable(JSON)') AS `notify`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`person_id`)), '')), 'Nullable(UUID)') AS `person_id`
from raw_source
