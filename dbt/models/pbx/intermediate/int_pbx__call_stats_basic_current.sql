{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_call_stats_basic',
    engine=ch_engine_replacing('started_at'),
    order_by='(entry_id, from_extension, to_extension, started_at)',
    unique_key=['entry_id', 'from_extension', 'to_extension', 'started_at'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'call_stats_basic', 'current'],
  )
}}

with raw_source as (
  select `entry_id`, `started_at`, `finish`, `answer`, `from_extension`, `from_number`, `to_extension`, `to_number`, `disconnect_reason`, `line_number`, `location`, `records`, `create_ts`, `raw_data`
  from {{ bronze_parquet('pbx', 'call_stats_basic', '`entry_id` Nullable(String), `started_at` String, `finish` Nullable(String), `answer` Nullable(String), `from_extension` Nullable(String), `from_number` Nullable(String), `to_extension` Nullable(String), `to_number` Nullable(String), `disconnect_reason` Nullable(String), `line_number` Nullable(String), `location` Nullable(String), `records` Nullable(String), `create_ts` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  CAST(`entry_id`, 'Nullable(String)') AS `entry_id`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `started_at`,
  CAST(`finish`, 'Nullable(String)') AS `finish`,
  CAST(`answer`, 'Nullable(String)') AS `answer`,
  CAST(`from_extension`, 'Nullable(String)') AS `from_extension`,
  CAST(`from_number`, 'Nullable(String)') AS `from_number`,
  CAST(`to_extension`, 'Nullable(String)') AS `to_extension`,
  CAST(`to_number`, 'Nullable(String)') AS `to_number`,
  CAST(`disconnect_reason`, 'Nullable(String)') AS `disconnect_reason`,
  CAST(`line_number`, 'Nullable(String)') AS `line_number`,
  CAST(`location`, 'Nullable(String)') AS `location`,
  CAST(`records`, 'Nullable(String)') AS `records`,
  CAST(`create_ts`, 'Nullable(String)') AS `create_ts`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`raw_data`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`raw_data`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`raw_data`)), ''), '}'), nullIf(trimBoth(toString(`raw_data`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `raw_data`
from raw_source