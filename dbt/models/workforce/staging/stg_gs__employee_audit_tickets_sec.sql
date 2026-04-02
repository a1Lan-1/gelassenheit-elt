{{
  config(
    materialized='view',
    alias='stg_employee_audit_tickets_sec',
    tags=['gs', 'employee_audit_tickets_sec', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `date`, `link`, `employee`, `score`
  from {{ s3_parquet_run('gs', 'employee_audit_tickets_sec', '`date` Nullable(String), `link` Nullable(String), `employee` Nullable(String), `score` Nullable(String)') }}
)
select
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`date`)), ''))), 'Nullable(Date)') AS `date`,
  CAST(`link`, 'Nullable(String)') AS `link`,
  CAST(`employee`, 'Nullable(String)') AS `employee`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`score`)), ''), ',', '.')), 'Nullable(Float64)') AS `score`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source
