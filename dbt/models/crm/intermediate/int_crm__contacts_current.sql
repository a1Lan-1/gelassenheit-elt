{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_contacts',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'contacts', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `first_name`, `last_name`, `responsible_user_id`, `group_id`, `created_by`, `updated_by`, `created_at`, `updated_at`, `is_deleted`, `account_id`, `phone`, `email`, `custom_fields_values`, `tags`, `leads`, `companies`, `raw_data`
  from {{ bronze_parquet('crm', 'contacts', '`id` Nullable(UInt64), `name` Nullable(String), `first_name` Nullable(String), `last_name` Nullable(String), `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `created_at` String, `updated_at` String, `is_deleted` Nullable(String), `account_id` Nullable(UInt64), `phone` Nullable(String), `email` Nullable(String), `custom_fields_values` Nullable(String), `tags` Nullable(String), `leads` Nullable(String), `companies` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`first_name`, 'Nullable(String)') AS `first_name`,
  CAST(`last_name`, 'Nullable(String)') AS `last_name`,
  CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
  CAST(`group_id`, 'Nullable(UInt64)') AS `group_id`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
  CAST(`is_deleted`, 'Nullable(String)') AS `is_deleted`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST(`phone`, 'Nullable(String)') AS `phone`,
  CAST(`email`, 'Nullable(String)') AS `email`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`tags`)), '')") }}, 'Nullable(JSON)') AS `tags`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`leads`)), '')") }}, 'Nullable(JSON)') AS `leads`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`companies`)), '')") }}, 'Nullable(JSON)') AS `companies`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
