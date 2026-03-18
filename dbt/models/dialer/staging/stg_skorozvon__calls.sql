{{
  config(
    materialized='table',
    alias='stg_calls',
    engine=ch_engine_merge_tree(),
    order_by='(id)',
    settings={'allow_nullable_key': 1},
    tags=['dialer', 'calls', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('dialer', 'calls', '`id` Nullable(UInt64), `started_at` Nullable(DateTime64(3)), `user_id` Nullable(UInt64), `user_name` Nullable(String), `phone` Nullable(String), `result` Nullable(String), `type` Nullable(String), `duration` Nullable(Int64), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(if(isNull(`started_at`), NULL, `started_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `started_at`,
  CAST(`user_id`, 'Nullable(UInt64)') AS `user_id`,
  CAST(`user_name`, 'Nullable(String)') AS `user_name`,
  CAST(`phone`, 'Nullable(String)') AS `phone`,
  CAST(`result`, 'Nullable(String)') AS `result`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`duration`, 'Nullable(Int64)') AS `duration`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
