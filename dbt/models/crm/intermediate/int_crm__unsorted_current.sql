{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_unsorted',
    engine=ch_engine_replacing('created_at'),
    order_by='(uid)',
    unique_key=['uid'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'unsorted', 'current'],
  )
}}

with raw_source as (
  select `uid`, `source_uid`, `category`, `pipeline_id`, `created_at`, `account_id`, `raw_data`
  from {{ bronze_parquet('crm', 'unsorted', '`uid` Nullable(String), `source_uid` Nullable(String), `category` Nullable(String), `pipeline_id` Nullable(UInt64), `created_at` String, `account_id` Nullable(UInt64), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`uid`, 'Nullable(String)')) AS `uid`,
  CAST(`source_uid`, 'Nullable(String)') AS `source_uid`,
  CAST(`category`, 'Nullable(String)') AS `category`,
  CAST(`pipeline_id`, 'Nullable(UInt64)') AS `pipeline_id`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `created_at`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
