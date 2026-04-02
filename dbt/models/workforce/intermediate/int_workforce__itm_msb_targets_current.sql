{{
  config(
    materialized='table',
    alias='itm_msb_targets',
    engine=ch_engine_merge_tree(),
    order_by='(target_name, employee_type, effective_date, target_value)',
    settings={'allow_nullable_key': 1},
    tags=['gs', 'itm_msb_targets', 'current'],
  )
}}

with raw_source as (
  select `target_name`, `employee_type`, `effective_date`, `target_value`, `weight`
  from {{ gs_bronze_parquet('itm_msb_targets', '`target_name` Nullable(String), `employee_type` Nullable(String), `effective_date` Nullable(String), `target_value` Nullable(String), `weight` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`target_name`, 'Nullable(String)')) AS `target_name`,
  assumeNotNull(CAST(`employee_type`, 'Nullable(String)')) AS `employee_type`,
  CAST(coalesce(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`effective_date`)), ''))), toDate('1970-01-01')), 'Date') AS `effective_date`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`target_value`)), ''), ',', '.')), 'Nullable(Float64)') AS `target_value`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`weight`)), ''), ',', '.')), 'Nullable(Float64)') AS `weight`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source