{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_task_status',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'task_status', 'current'],
  )
}}

with raw_source as (
  select `id`, `status_id`, `task_id`, `actual`, `user_id`, `data`, `created_at`, `updated_at`, `deleted_at`, `date_begin`, `date_end`, `comment`
  from {{ bronze_parquet('helpdesk', 'task_status', '`id` String, `status_id` String, `task_id` String, `actual` Nullable(String), `user_id` String, `data` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `date_begin` String, `date_end` String, `comment` Nullable(String)') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`status_id`)), '')), 'Nullable(UUID)') AS `status_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`task_id`)), '')), 'Nullable(UUID)') AS `task_id`,
  CAST(multiIf(lower(trimBoth(toString(`actual`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`actual`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`actual`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`actual`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `actual`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_id`)), '')), 'Nullable(UUID)') AS `user_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`date_begin`)), ''), 3), 'Nullable(DateTime64(3))') AS `date_begin`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`date_end`)), ''), 3), 'Nullable(DateTime64(3))') AS `date_end`,
  CAST(`comment`, 'Nullable(String)') AS `comment`
from raw_source