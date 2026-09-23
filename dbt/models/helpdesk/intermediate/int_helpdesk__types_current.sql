{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_types',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'types', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `code`, `table_name`, `table_type`, `description`, `created_at`, `updated_at`, `deleted_at`
  from {{ bronze_parquet('helpdesk', 'types', '`id` String, `name` Nullable(String), `code` Nullable(String), `table_name` Nullable(String), `table_type` Nullable(String), `description` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(`name`, 'Nullable(String)')) AS `name`,
  assumeNotNull(CAST(`code`, 'Nullable(String)')) AS `code`,
  assumeNotNull(CAST(`table_name`, 'Nullable(String)')) AS `table_name`,
  CAST(`table_type`, 'Nullable(String)') AS `table_type`,
  CAST(`description`, 'Nullable(String)') AS `description`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`
from raw_source