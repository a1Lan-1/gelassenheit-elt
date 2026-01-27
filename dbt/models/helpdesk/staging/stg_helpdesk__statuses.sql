{{
  config(
    materialized='view',
    alias='stg_statuses',
    tags=['helpdesk', 'statuses', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `external_id`, `name`, `code`, `description`, `data`, `created_at`, `updated_at`, `deleted_at`, `sla`, `table_name`, `table_type`, `order`, `default`
  from {{ s3_parquet_run('helpdesk', 'statuses', '`id` String, `external_id` Nullable(String), `name` Nullable(String), `code` Nullable(String), `description` Nullable(String), `data` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `sla` Nullable(String), `table_name` Nullable(String), `table_type` Nullable(String), `order` Nullable(Int32), `default` Nullable(String)') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(`external_id`, 'Nullable(String)') AS `external_id`,
  assumeNotNull(CAST(`name`, 'Nullable(String)')) AS `name`,
  assumeNotNull(CAST(`code`, 'Nullable(String)')) AS `code`,
  CAST(`description`, 'Nullable(String)') AS `description`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(multiIf(lower(trimBoth(toString(`sla`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`sla`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`sla`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`sla`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `sla`,
  CAST(`table_name`, 'Nullable(String)') AS `table_name`,
  CAST(`table_type`, 'Nullable(String)') AS `table_type`,
  CAST(`order`, 'Nullable(Int32)') AS `order`,
  assumeNotNull(CAST(multiIf(lower(trimBoth(toString(`default`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`default`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`default`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`default`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)')) AS `default`
from raw_source
