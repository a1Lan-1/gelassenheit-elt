{{
  config(
    materialized='table',
    alias='esp_techsup_schedule',
    engine=ch_engine_merge_tree(),
    order_by='(date, employee)',
    settings={'allow_nullable_key': 1},
    tags=['gs', 'esp_techsup_schedule', 'current'],
  )
}}

with raw_source as (
  select `employee`, `date`, `hours`, `loaded_at`
  from {{ gs_bronze_parquet('esp_techsup_schedule', '`employee` Nullable(String), `date` String, `hours` Nullable(String), `loaded_at` String') }}
)
select
  assumeNotNull(CAST(`employee`, 'Nullable(String)')) AS `employee`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`date`)), ''), 3), 'Nullable(DateTime64(3))') AS `date`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`hours`)), ''), ',', '.')), 'Nullable(Float64)') AS `hours`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`loaded_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `loaded_at`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source