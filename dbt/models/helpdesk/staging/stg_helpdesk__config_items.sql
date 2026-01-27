{{
  config(
    materialized='view',
    alias='stg_ci',
    tags=['helpdesk', 'config_items', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `sku_id`, `place_id`, `parent_id`, `item_id`, `quantity`, `cost`, `responsible_id`, `activation_date`, `validity_date`, `inventory_number`, `comment`, `serial_number`, `data`, `expired_at`, `created_at`, `updated_at`, `deleted_at`, `waybill_in`, `waybill_out`, `state`, `weight`, `supplier`, `responsible_group_id`, `pmsr_id`
  from {{ s3_parquet_run('helpdesk', 'config_items', '`id` String, `sku_id` String, `place_id` String, `parent_id` String, `item_id` String, `quantity` Nullable(Int32), `cost` Nullable(String), `responsible_id` String, `activation_date` String, `validity_date` String, `inventory_number` Nullable(String), `comment` Nullable(String), `serial_number` Nullable(String), `data` Nullable(String), `expired_at` String, `created_at` String, `updated_at` String, `deleted_at` String, `waybill_in` String, `waybill_out` String, `state` Nullable(String), `weight` Nullable(String), `supplier` Nullable(String), `responsible_group_id` String, `pmsr_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`sku_id`)), '')), 'Nullable(UUID)')) AS `sku_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`place_id`)), '')), 'Nullable(UUID)') AS `place_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`parent_id`)), '')), 'Nullable(UUID)') AS `parent_id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`item_id`)), '')), 'Nullable(UUID)')) AS `item_id`,
  assumeNotNull(CAST(`quantity`, 'Nullable(Int32)')) AS `quantity`,
  assumeNotNull(CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`cost`)), ''), ',', '.')), 'Nullable(Float64)')) AS `cost`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`responsible_id`)), '')), 'Nullable(UUID)') AS `responsible_id`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`activation_date`)), ''), 3), 'Nullable(DateTime64(3))') AS `activation_date`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`validity_date`)), ''), 3), 'Nullable(DateTime64(3))') AS `validity_date`,
  CAST(`inventory_number`, 'Nullable(String)') AS `inventory_number`,
  CAST(`comment`, 'Nullable(String)') AS `comment`,
  CAST(`serial_number`, 'Nullable(String)') AS `serial_number`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`expired_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `expired_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`waybill_in`)), '')), 'Nullable(UUID)')) AS `waybill_in`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`waybill_out`)), '')), 'Nullable(UUID)') AS `waybill_out`,
  CAST(`state`, 'Nullable(String)') AS `state`,
  assumeNotNull(CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`weight`)), ''), ',', '.')), 'Nullable(Float64)')) AS `weight`,
  CAST(`supplier`, 'Nullable(String)') AS `supplier`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`responsible_group_id`)), '')), 'Nullable(UUID)') AS `responsible_group_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`pmsr_id`)), '')), 'Nullable(UUID)') AS `pmsr_id`
from raw_source
