{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_users',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'users', 'current'],
  )
}}

with raw_source as (
  select `id`, `user_name`, `first_name`, `middle_name`, `last_name`, `email`, `phone`, `created_at`, `updated_at`, `deleted_at`, `oauth_id`, `temp_password`, `type`, `roles`, `blocked`, `type_id`
  from {{ bronze_parquet('helpdesk', 'users', '`id` String, `user_name` Nullable(String), `first_name` Nullable(String), `middle_name` Nullable(String), `last_name` Nullable(String), `email` Nullable(String), `phone` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `oauth_id` Nullable(String), `temp_password` Nullable(String), `type` Nullable(String), `roles` Nullable(String), `blocked` Nullable(String), `type_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(`user_name`, 'Nullable(String)')) AS `user_name`,
  assumeNotNull(CAST(`first_name`, 'Nullable(String)')) AS `first_name`,
  CAST(`middle_name`, 'Nullable(String)') AS `middle_name`,
  assumeNotNull(CAST(`last_name`, 'Nullable(String)')) AS `last_name`,
  CAST(`email`, 'Nullable(String)') AS `email`,
  CAST(`phone`, 'Nullable(String)') AS `phone`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(`oauth_id`, 'Nullable(String)') AS `oauth_id`,
  CAST(`temp_password`, 'Nullable(String)') AS `temp_password`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`roles`)), '')") }}, 'Nullable(JSON)') AS `roles`,
  assumeNotNull(CAST(multiIf(lower(trimBoth(toString(`blocked`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`blocked`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`blocked`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`blocked`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)')) AS `blocked`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`type_id`)), '')), 'Nullable(UUID)') AS `type_id`
from raw_source