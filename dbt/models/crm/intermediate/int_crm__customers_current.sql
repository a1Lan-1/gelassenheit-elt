{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_customers',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'customers', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `next_price`, `next_date`, `responsible_user_id`, `status_id`, `periodicity`, `created_by`, `updated_by`, `created_at`, `updated_at`, `account_id`, `ltv`, `purchases_count`, `average_check`, `custom_fields_values`, `tags`, `raw_data`
  from {{ bronze_parquet('crm', 'customers', '`id` Nullable(UInt64), `name` Nullable(String), `next_price` Nullable(Int64), `next_date` String, `responsible_user_id` Nullable(UInt64), `status_id` Nullable(UInt64), `periodicity` Nullable(Int64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` String, `updated_at` String, `account_id` Nullable(UInt64), `ltv` Nullable(Int64), `purchases_count` Nullable(Int64), `average_check` Nullable(Int64), `custom_fields_values` Nullable(String), `tags` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`next_price`, 'Nullable(Int64)') AS `next_price`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`next_date`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`next_date`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `next_date`,
  CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
  CAST(`status_id`, 'Nullable(UInt64)') AS `status_id`,
  CAST(`periodicity`, 'Nullable(Int64)') AS `periodicity`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST(`ltv`, 'Nullable(Int64)') AS `ltv`,
  CAST(`purchases_count`, 'Nullable(Int64)') AS `purchases_count`,
  CAST(`average_check`, 'Nullable(Int64)') AS `average_check`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`tags`)), '')") }}, 'Nullable(JSON)') AS `tags`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
