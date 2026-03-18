{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_calls',
    engine=ch_engine_replacing('started_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['dialer', 'calls', 'current'],
  )
}}

with raw_source as (
  select `id`, `started_at`, `region`, `phone`, `call_type`, `call_type_code`, `missed_reason`, `duration`, `recording_url`, `source`, `waiting_time`, `waiting_on_line_time`, `terminator`, `cost`, `duration_without_holding`, `duration_holding`, `duration_billed`, `user_id`, `user_name`, `organization`, `lead`, `scenario`, `scenario_result`, `scenario_result_group`, `main_lead`, `event`, `call_project`, `transfer_initiator`, `transfer_initiator_id`, `external_access_id`, `reason`, `raw_data`
  from {{ bronze_parquet('dialer', 'calls', '`id` Nullable(UInt64), `started_at` String, `region` Nullable(String), `phone` Nullable(String), `call_type` Nullable(String), `call_type_code` Nullable(String), `missed_reason` Nullable(String), `duration` Nullable(Int64), `recording_url` Nullable(String), `source` Nullable(String), `waiting_time` Nullable(Int64), `waiting_on_line_time` Nullable(String), `terminator` Nullable(String), `cost` Nullable(String), `duration_without_holding` Nullable(Int64), `duration_holding` Nullable(Int64), `duration_billed` Nullable(String), `user_id` Nullable(UInt64), `user_name` Nullable(String), `organization` Nullable(String), `lead` Nullable(String), `scenario` Nullable(String), `scenario_result` Nullable(String), `scenario_result_group` Nullable(String), `main_lead` Nullable(String), `event` Nullable(String), `call_project` Nullable(String), `transfer_initiator` Nullable(String), `transfer_initiator_id` Nullable(Int64), `external_access_id` Nullable(String), `reason` Nullable(String), `raw_data` Nullable(String)') }}
),
typed as (
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `started_at`,
  CAST(`region`, 'Nullable(String)') AS `region`,
  CAST(`phone`, 'Nullable(String)') AS `phone`,
  CAST(`call_type`, 'Nullable(String)') AS `call_type`,
  CAST(`call_type_code`, 'Nullable(String)') AS `call_type_code`,
  CAST(`missed_reason`, 'Nullable(String)') AS `missed_reason`,
  CAST(`duration`, 'Nullable(Int64)') AS `duration`,
  CAST(`recording_url`, 'Nullable(String)') AS `recording_url`,
  CAST(`source`, 'Nullable(String)') AS `source`,
  CAST(`waiting_time`, 'Nullable(Int64)') AS `waiting_time`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`waiting_on_line_time`)), ''), ',', '.')), 'Nullable(Float64)') AS `waiting_on_line_time`,
  CAST(`terminator`, 'Nullable(String)') AS `terminator`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`cost`)), ''), ',', '.')), 'Nullable(Float64)') AS `cost`,
  CAST(`duration_without_holding`, 'Nullable(Int64)') AS `duration_without_holding`,
  CAST(`duration_holding`, 'Nullable(Int64)') AS `duration_holding`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`duration_billed`)), ''), ',', '.')), 'Nullable(Float64)') AS `duration_billed`,
  CAST(`user_id`, 'Nullable(UInt64)') AS `user_id`,
  CAST(`user_name`, 'Nullable(String)') AS `user_name`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`organization`)), '')") }}, 'Nullable(JSON)') AS `organization`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`lead`)), '')") }}, 'Nullable(JSON)') AS `lead`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`scenario`)), '')") }}, 'Nullable(JSON)') AS `scenario`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`scenario_result`)), '')") }}, 'Nullable(JSON)') AS `scenario_result`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`scenario_result_group`)), '')") }}, 'Nullable(JSON)') AS `scenario_result_group`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`main_lead`)), '')") }}, 'Nullable(JSON)') AS `main_lead`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`event`)), '')") }}, 'Nullable(JSON)') AS `event`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`call_project`)), '')") }}, 'Nullable(JSON)') AS `call_project`,
  CAST(`transfer_initiator`, 'Nullable(String)') AS `transfer_initiator`,
  CAST(`transfer_initiator_id`, 'Nullable(Int64)') AS `transfer_initiator_id`,
  CAST(`external_access_id`, 'Nullable(String)') AS `external_access_id`,
  CAST(`reason`, 'Nullable(String)') AS `reason`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
)
-- Skip broken telemetry on incremental loads (null wait is allowed).
select *
from typed
where waiting_on_line_time is null
   or waiting_on_line_time >= 0
