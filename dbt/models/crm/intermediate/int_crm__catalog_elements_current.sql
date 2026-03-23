{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_catalog_elements',
    engine=ch_engine_replacing('updated_at'),
    order_by='(catalog_id, id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'catalog_elements', 'current'],
  )
}}

with raw_source as (
  select `id`, `catalog_id`, `name`, `created_by`, `updated_by`, `created_at`, `updated_at`, `account_id`, `custom_fields_values`, `raw_data`
  from {{ bronze_parquet('crm', 'catalog_elements', '`id` Nullable(UInt64), `catalog_id` Nullable(UInt64), `name` Nullable(String), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` String, `updated_at` String, `account_id` Nullable(UInt64), `custom_fields_values` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`catalog_id`, 'Nullable(UInt64)') AS `catalog_id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
