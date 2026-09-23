{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_sku',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'sku', 'current'],
  )
}}

with raw_source as (
  select `id`, `type`, `manufacturer`, `model`, `version`, `unit`, `validity_period`, `data`, `created_at`, `updated_at`, `deleted_at`, `weight`, `vendor_code`, `name`, `label_template`, `is_digital`, `cost`, `is_package`
  from {{ bronze_parquet('helpdesk', 'sku', '`id` String, `type` Nullable(String), `manufacturer` Nullable(String), `model` Nullable(String), `version` Nullable(String), `unit` Nullable(String), `validity_period` Nullable(Int32), `data` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `weight` Nullable(String), `vendor_code` Nullable(String), `name` Nullable(String), `label_template` String, `is_digital` Nullable(String), `cost` Nullable(String), `is_package` Nullable(String)') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(`type`, 'Nullable(String)')) AS `type`,
  CAST(`manufacturer`, 'Nullable(String)') AS `manufacturer`,
  CAST(`model`, 'Nullable(String)') AS `model`,
  CAST(`version`, 'Nullable(String)') AS `version`,
  assumeNotNull(CAST(`unit`, 'Nullable(String)')) AS `unit`,
  CAST(`validity_period`, 'Nullable(Int32)') AS `validity_period`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  assumeNotNull(CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`weight`)), ''), ',', '.')), 'Nullable(Float64)')) AS `weight`,
  CAST(`vendor_code`, 'Nullable(String)') AS `vendor_code`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`label_template`)), '')), 'Nullable(UUID)') AS `label_template`,
  assumeNotNull(CAST(multiIf(lower(trimBoth(toString(`is_digital`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`is_digital`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`is_digital`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`is_digital`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)')) AS `is_digital`,
  assumeNotNull(CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`cost`)), ''), ',', '.')), 'Nullable(Float64)')) AS `cost`,
  assumeNotNull(CAST(multiIf(lower(trimBoth(toString(`is_package`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`is_package`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`is_package`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`is_package`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)')) AS `is_package`
from raw_source