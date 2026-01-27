{{
  config(
    materialized='view',
    alias='stg_task_ci',
    tags=['helpdesk', 'task_config_item', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `task_id`, `config_item_id`, `created_at`, `updated_at`, `deleted_at`
  from {{ s3_parquet_run('helpdesk', 'task_config_item', '`id` String, `task_id` String, `config_item_id` String, `created_at` String, `updated_at` String, `deleted_at` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`task_id`)), '')), 'Nullable(UUID)')) AS `task_id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`config_item_id`)), '')), 'Nullable(UUID)')) AS `config_item_id`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`
from raw_source
