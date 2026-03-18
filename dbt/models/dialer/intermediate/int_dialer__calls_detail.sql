{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    alias='cur_calls_detail',
    engine=ch_engine_merge_tree(),
    order_by='(id)',
    settings={'allow_nullable_key': 1},
    tags=['dialer', 'calls', 'detail'],
  )
}}

- Increment: lookback by started_at (hours); full-refresh - the whole story.
{% set lookback_hours = var('dialer_calls_detail_lookback_hours', 6) | int %}

-- JSON path (col.key) works on table/view columns only — not on CTE aliases.
with src as (
  select
    id,
    started_at,
    phone,
    duration,
    missed_reason,
    source,
    transfer_initiator,
    transfer_initiator_id,
    lead as lead_json,
    scenario_result as scenario_result_json,
    coalesce(
      nullIf(user_id, 0),
      toUInt64OrNull(ifNull(toString(raw_data.user.id), ''))
    ) as user_id,
    coalesce(
      nullIf(trimBoth(user_name), ''),
      nullIf(ifNull(toString(raw_data.user.name), ''), '')
    ) as user_name,
    coalesce(
      nullIf(call_type_code, ''),
      nullIf(ifNull(toString(raw_data.call_type_code), ''), '')
    ) as call_type_code,
    coalesce(
      waiting_on_line_time,
      toFloat64OrNull(ifNull(toString(raw_data.waiting_on_line_time), ''))
    ) as waiting_on_line_time,
    ifNull(toString(scenario_result.name), '') as scenario_result_name,
    ifNull(toString(lead.phones), '') as lead_phones
  from {{ ref('int_dialer__calls_current') }} final
  {% if is_incremental() %}
  where started_at >= now() - toIntervalHour({{ lookback_hours }})
  {% endif %}
)

-- Exclude broken telemetry: negative waiting_on_line_time must not enter analytics.
select
  id,
  started_at,
  user_id,
  user_name,
  phone,
  duration,
  call_type_code,
  missed_reason,
  source,
  waiting_on_line_time,
  lead_json,
  scenario_result_json,
  scenario_result_name,
  transfer_initiator,
  transfer_initiator_id,
  coalesce(
    nullIf(
      multiIf(call_type_code = 'outgoing', phone, source),
      ''
    ),
    replaceRegexpAll(lead_phones, '[^0-9+]', '')
  ) as client_phone
from src
where waiting_on_line_time is null
   or waiting_on_line_time >= 0
