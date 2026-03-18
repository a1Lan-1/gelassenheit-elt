{{
  config(
    materialized='table',
    alias='stg_hours',
    engine=ch_engine_merge_tree(),
    order_by='(date_h, user_id)',
    settings={'allow_nullable_key': 1},
    tags=['dialer', 'hours', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('dialer', 'hours', '`date_h` Nullable(DateTime64(3)), `date` Nullable(Date), `user_id` Nullable(UInt64), `full_name` Nullable(String), `email` Nullable(String), `status_normal` Nullable(Int64), `status_ringing` Nullable(Int64), `status_speaking` Nullable(Int64), `status_wrapup` Nullable(Int64), `status_away` Nullable(Int64), `status_dnd` Nullable(Int64), `status_accident` Nullable(Int64), `status_offline` Nullable(Int64), `performance_seconds` Nullable(Int64), `performance_hours` Nullable(Float64), `performance_occ` Nullable(Float64)') }}
)
select
  CAST(if(isNull(`date_h`), NULL, `date_h` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `date_h`,
  CAST(`date`, 'Nullable(Date)') AS `date`,
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
  CAST(`performance_hours`, 'Nullable(Float64)') AS `performance_hours`,
  CAST(`performance_occ`, 'Nullable(Float64)') AS `performance_occ`
from raw_source
