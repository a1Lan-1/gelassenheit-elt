{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_services',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'services', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `code`, `description`, `data`, `user_id`, `user_group_id`, `created_at`, `updated_at`, `deleted_at`, `parent_id`
  from {{ bronze_parquet('helpdesk', 'services', '`id` String, `name` Nullable(String), `code` Nullable(String), `description` Nullable(String), `data` Nullable(String), `user_id` String, `user_group_id` String, `created_at` String, `updated_at` String, `deleted_at` String, `parent_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(`name`, 'Nullable(String)')) AS `name`,
  CAST(`code`, 'Nullable(String)') AS `code`,
  CAST(`description`, 'Nullable(String)') AS `description`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_id`)), '')), 'Nullable(UUID)') AS `user_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_group_id`)), '')), 'Nullable(UUID)') AS `user_group_id`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`parent_id`)), '')), 'Nullable(UUID)') AS `parent_id`
from raw_source