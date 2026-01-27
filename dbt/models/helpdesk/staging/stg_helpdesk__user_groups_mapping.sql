{{
  config(
    materialized='view',
    alias='stg_ug_mapping',
    tags=['helpdesk', 'user_groups_mapping', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `user_id`, `user_group_id`, `type`, `created_at`, `updated_at`, `deleted_at`
  from {{ s3_parquet_run('helpdesk', 'user_groups_mapping', '`id` String, `user_id` String, `user_group_id` String, `type` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_id`)), '')), 'Nullable(UUID)')) AS `user_id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_group_id`)), '')), 'Nullable(UUID)')) AS `user_group_id`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`
from raw_source
