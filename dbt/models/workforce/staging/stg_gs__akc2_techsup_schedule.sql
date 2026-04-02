{{
  config(
    materialized='view',
    alias='stg_akc2_techsup_schedule',
    tags=['gs', 'akc2_techsup_schedule', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `date`, `user_name`, `work_hours`
  from {{ s3_parquet_run('gs', 'akc2_techsup_schedule', '`date` Nullable(String), `user_name` Nullable(String), `work_hours` Nullable(String)') }}
)
select
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`date`)), ''))), 'Nullable(Date)') AS `date`,
  CAST(`user_name`, 'Nullable(String)') AS `user_name`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`work_hours`)), ''), ',', '.')), 'Nullable(Float64)') AS `work_hours`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source
