{{
  config(
    materialized='table',
    alias='esp_techsup_employees',
    engine=ch_engine_merge_tree(),
    order_by='(full_name)',
    settings={'allow_nullable_key': 1},
    tags=['gs', 'esp_techsup_employees', 'current'],
  )
}}

with raw_source as (
  select `full_name`, `line`, `supervisor`, `hired_at`, `birthday`, `employment_status`
  from {{ gs_bronze_parquet('esp_techsup_employees', '`full_name` Nullable(String), `line` Nullable(String), `supervisor` Nullable(String), `hired_at` Nullable(String), `birthday` Nullable(String), `employment_status` Nullable(String)') }}
),
deduped as (
  select
    *,
    row_number() over (
      partition by lower(trimBoth(toString(`full_name`)))
      order by `supervisor`, `line`
    ) as rn
  from raw_source
  where nullIf(trimBoth(toString(`full_name`)), '') is not null
)
select
  assumeNotNull(CAST(`full_name`, 'Nullable(String)')) AS `full_name`,
  CAST(`line`, 'Nullable(String)') AS `line`,
  CAST(`supervisor`, 'Nullable(String)') AS `supervisor`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`hired_at`)), ''))), 'Nullable(Date)') AS `hired_at`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`birthday`)), ''))), 'Nullable(Date)') AS `birthday`,
  CAST(multiIf(lower(trimBoth(toString(`employment_status`))) IN ('true', '1', 'yes', 'y', 't'), 'Terminated', lower(trimBoth(toString(`employment_status`))) IN ('false', '0', 'no', 'n', 'f'), 'Active', nullIf(trimBoth(toString(`employment_status`)), '')), 'Nullable(String)') AS `employment_status`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from deduped
where rn = 1