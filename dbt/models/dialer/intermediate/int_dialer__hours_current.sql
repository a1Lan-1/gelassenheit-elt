{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_hours',
    engine=ch_engine_replacing('date_h'),
    order_by='(date_h, user_id)',
    unique_key=['date_h', 'user_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['dialer', 'hours', 'current'],
  )
}}

with raw_source as (
  select `date_h`, `date`, `user_id`, `full_name`, `email`, `status_normal`, `status_ringing`, `status_speaking`, `status_wrapup`, `status_away`, `status_dnd`, `status_accident`, `status_offline`, `performance_seconds`, `performance_hours`, `performance_occ`
  from {{ bronze_parquet('dialer', 'hours', '`date_h` String, `date` Nullable(String), `user_id` Nullable(UInt64), `full_name` Nullable(String), `email` Nullable(String), `status_normal` Nullable(Int64), `status_ringing` Nullable(Int64), `status_speaking` Nullable(Int64), `status_wrapup` Nullable(Int64), `status_away` Nullable(Int64), `status_dnd` Nullable(Int64), `status_accident` Nullable(Int64), `status_offline` Nullable(Int64), `performance_seconds` Nullable(Int64), `performance_hours` Nullable(String), `performance_occ` Nullable(String)') }}
  where coalesce(toUInt64OrNull(toString(`user_id`)), 0) > 0
),
parsed as (
  select
    *,
    nullIf(trimBoth(toString(`date`)), '') as date_raw
  from raw_source
)
select
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`date_h`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`date_h`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `date_h`,
  CAST(
    coalesce(
      toDateOrNull(date_raw),
      if(
        match(date_raw, '^\\d{2}\\.\\d{2}\\.\\d{4}$'),
        toDate(parseDateTimeBestEffortOrNull(date_raw)),
        NULL
      )
    ),
    'Nullable(Date)'
  ) AS `date`,
  assumeNotNull(CAST(`user_id`, 'Nullable(UInt64)')) AS `user_id`,
  CAST(`full_name`, 'Nullable(String)') AS `full_name`,
  CAST(`email`, 'Nullable(String)') AS `email`,
  CAST(`status_normal`, 'Nullable(Int64)') AS `status_normal`,
  CAST(`status_ringing`, 'Nullable(Int64)') AS `status_ringing`,
  CAST(`status_speaking`, 'Nullable(Int64)') AS `status_speaking`,
  CAST(`status_wrapup`, 'Nullable(Int64)') AS `status_wrapup`,
  CAST(`status_away`, 'Nullable(Int64)') AS `status_away`,
  CAST(`status_dnd`, 'Nullable(Int64)') AS `status_dnd`,
  CAST(`status_accident`, 'Nullable(Int64)') AS `status_accident`,
  CAST(`status_offline`, 'Nullable(Int64)') AS `status_offline`,
  CAST(`performance_seconds`, 'Nullable(Int64)') AS `performance_seconds`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`performance_hours`)), ''), ',', '.')), 'Nullable(Float64)') AS `performance_hours`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`performance_occ`)), ''), ',', '.')), 'Nullable(Float64)') AS `performance_occ`
from parsed
