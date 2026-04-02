{{
  config(
    materialized='table',
    alias='akc1_techsup_schedule',
    engine=ch_engine_merge_tree(),
    order_by='(date, employee)',
    settings={'allow_nullable_key': 1},
    tags=['gs', 'akc1_techsup_schedule', 'current'],
  )
}}

with raw_source as (
  select `date`, `employee`, `hours`
  from {{ gs_bronze_parquet('akc1_techsup_schedule', '`date` Nullable(String), `employee` Nullable(String), `hours` Nullable(String)') }}
)
select
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`date`)), ''))), 'Nullable(Date)') AS `date`,
  CAST(`employee`, 'Nullable(String)') AS `employee`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`hours`)), ''), ',', '.')), 'Nullable(Float64)') AS `hours`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source