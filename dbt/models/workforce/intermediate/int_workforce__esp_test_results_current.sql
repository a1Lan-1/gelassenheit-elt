{{
  config(
    materialized='table',
    alias='esp_test_results',
    engine=ch_engine_merge_tree(),
    order_by='(name, dt)',
    settings={'allow_nullable_key': 1},
    tags=['gs', 'esp_test_results', 'current'],
  )
}}

{# As QA (employee_audit_tickets_sec): bronze date + toDate(parseDateTimeBestEffort).
   The column in the parquet is called date, not dt — otherwise CH replaces the value of hive-partition dt=. #}
with raw_source as (
  select `name`, `date`, `n_questions`, `result`
  from {{ gs_bronze_parquet('esp_test_results', '`name` Nullable(String), `date` Nullable(String), `n_questions` Nullable(String), `result` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`name`, 'Nullable(String)')) AS `name`,
  CAST(toDate(parseDateTimeBestEffortOrNull(nullIf(trimBoth(toString(`date`)), ''))), 'Nullable(Date)') AS `dt`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`n_questions`)), ''), ',', '.')), 'Nullable(Float64)') AS `n_questions`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`result`)), ''), ',', '.')), 'Nullable(Float64)') AS `result`,
  assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`
from raw_source
