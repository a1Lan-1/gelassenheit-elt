{{
  config(
    materialized='table',
    alias='stg_calls',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id)',
    settings={'allow_nullable_key': 1},
    tags=['pbx', 'calls', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('pbx', 'calls', '`entry_id` Nullable(String), `started_at` Nullable(DateTime64(3)), `context_type` Nullable(String), `context_status` Nullable(String), `caller_id` Nullable(String), `caller_name` Nullable(String), `caller_number` Nullable(String), `called_number` Nullable(String), `duration` Nullable(Int64), `talk_duration` Nullable(Int64), `context_init_type` Nullable(String), `recall_status` Nullable(String), `cost` Nullable(Float64), `context_cost_full` Nullable(Float64), `context_cost_tariff` Nullable(Float64), `recording_ids` Nullable(String), `period` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`entry_id`, 'Nullable(String)')) AS `entry_id`,
  CAST(`started_at`, 'Nullable(DateTime64(3))') AS `started_at`,
  CAST(`context_type`, 'Nullable(String)') AS `context_type`,
  CAST(`context_status`, 'Nullable(String)') AS `context_status`,
  CAST(`caller_id`, 'Nullable(String)') AS `caller_id`,
  CAST(`caller_name`, 'Nullable(String)') AS `caller_name`,
  CAST(`caller_number`, 'Nullable(String)') AS `caller_number`,
  CAST(`called_number`, 'Nullable(String)') AS `called_number`,
  CAST(`duration`, 'Nullable(Int64)') AS `duration`,
  CAST(`talk_duration`, 'Nullable(Int64)') AS `talk_duration`,
  CAST(`context_init_type`, 'Nullable(String)') AS `context_init_type`,
  CAST(`recall_status`, 'Nullable(String)') AS `recall_status`,
  CAST(`cost`, 'Nullable(Float64)') AS `cost`,
  CAST(`context_cost_full`, 'Nullable(Float64)') AS `context_cost_full`,
  CAST(`context_cost_tariff`, 'Nullable(Float64)') AS `context_cost_tariff`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`recording_ids`)), '')") }}, 'Nullable(JSON)') AS `recording_ids`,
  CAST(`period`, 'Nullable(String)') AS `period`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
