{{
  config(
    materialized='view',
    alias='stg_akc_employees',
    tags=['gs', 'akc_employees', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `supervisor`, `employment_status`, `group_name`, `training_start_date`, `training_end_date`, `line_start_date`, `full_name`, `email`, `telegram_login`, `mm_login`, `phone`, `birthday`
  from {{ s3_parquet_run('gs', 'akc_employees', '`supervisor` Nullable(String), `employment_status` Nullable(String), `group_name` Nullable(String), `training_start_date` Nullable(String), `training_end_date` Nullable(String), `line_start_date` Nullable(String), `full_name` Nullable(String), `email` Nullable(String), `telegram_login` Nullable(String), `mm_login` Nullable(String), `phone` Nullable(String), `birthday` Nullable(String)') }}
)
select
  CAST(`supervisor`, 'Nullable(String)') AS `supervisor`,
  CAST(multiIf(lower(trimBoth(toString(`employment_status`))) IN ('true', '1', 'yes', 'y', 't'), 'Terminated', lower(trimBoth(toString(`employment_status`))) IN ('false', '0', 'no', 'n', 'f'), 'Active', nullIf(trimBoth(toString(`employment_status`)), '')), 'Nullable(String)') AS `employment_status`,
  CAST(`group_name`, 'Nullable(String)') AS `group_name`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`training_start_date`)), ''))), 'Nullable(Date)') AS `training_start_date`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`training_end_date`)), ''))), 'Nullable(Date)') AS `training_end_date`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`line_start_date`)), ''))), 'Nullable(Date)') AS `line_start_date`,
  CAST(`full_name`, 'Nullable(String)') AS `full_name`,
  CAST(`email`, 'Nullable(String)') AS `email`,
  CAST(`telegram_login`, 'Nullable(String)') AS `telegram_login`,
  CAST(`mm_login`, 'Nullable(String)') AS `mm_login`,
  CAST(`phone`, 'Nullable(String)') AS `phone`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`birthday`)), ''))), 'Nullable(Date)') AS `birthday`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source
