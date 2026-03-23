{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_events',
    engine=ch_engine_replacing('created_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'events', 'current'],
  )
}}

with raw_source as (
  select `id`, `type`, `entity_id`, `entity_type`, `created_by`, `created_at`, `value_after`, `value_before`, `account_id`, `entity_name`, `embedded_entity`
  from {{ bronze_parquet('crm', 'events', '`id` Nullable(String), `type` Nullable(String), `entity_id` Nullable(UInt64), `entity_type` Nullable(String), `created_by` Nullable(UInt64), `created_at` String, `value_after` Nullable(String), `value_before` Nullable(String), `account_id` Nullable(UInt64), `entity_name` Nullable(String), `embedded_entity` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(String)')) AS `id`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`entity_id`, 'Nullable(UInt64)') AS `entity_id`,
  CAST(`entity_type`, 'Nullable(String)') AS `entity_type`,
  CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `created_at`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`value_after`)), '')") }}, 'Nullable(JSON)') AS `value_after`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`value_before`)), '')") }}, 'Nullable(JSON)') AS `value_before`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST(`entity_name`, 'Nullable(String)') AS `entity_name`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`embedded_entity`)), '')") }}, 'Nullable(JSON)') AS `embedded_entity`
from raw_source