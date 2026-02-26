{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='entry_id',
    alias='cur_call_conversions',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id)',
    settings={'allow_nullable_key': 1},
    tags=['pbx', 'calls', 'cc', 'current'],
  )
}}

-- CC requests from CallContext.conversion (appears when ext_params=1).
-- Increment: delete+insert by entry_id for lookback hours.
{% set lookback_hours = var('pbx_legs_lookback_hours', 6) | int %}

with calls as (
  select
    entry_id,
    started_at,
    toString(raw_data) as raw_str
  from {{ ref('int_pbx__calls_current') }}
  final
  {% if is_incremental() %}
  where started_at >= now() - toIntervalHour({{ lookback_hours }})
  {% endif %}
),
src as (
  select
    entry_id,
    started_at,
    nullIf(JSONExtractRaw(ifNull(raw_str, '{}'), 'conversion'), '') as conv_raw
  from calls
),
parsed as (
  select
    entry_id,
    started_at,
    conv_raw,
    nullIf(toString(JSONExtractInt(conv_raw, 'conversion_id')), '0') as conversion_id,
    JSONExtractInt(conv_raw, 'channel_type') as channel_type,
    JSONExtractInt(conv_raw, 'result') as result_code,
    nullIf(toString(JSONExtractInt(conv_raw, 'assign_user_id')), '0') as assign_user_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'close_user_id')), '0') as close_user_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'contact_id')), '0') as contact_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'group_id')), '0') as group_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'deal_id')), '0') as deal_id,
    nullIf(JSONExtractString(conv_raw, 'entry_point'), '') as entry_point,
    coalesce(
      JSONExtractInt(conv_raw, 'first_answer'),
      toInt64OrNull(JSONExtractString(conv_raw, 'first_answer'))
    ) as first_answer_raw,
    coalesce(
      JSONExtractInt(conv_raw, 'start'),
      toInt64OrNull(JSONExtractString(conv_raw, 'start'))
    ) as taken_raw,
    coalesce(
      JSONExtractInt(conv_raw, 'create'),
      toInt64OrNull(JSONExtractString(conv_raw, 'create'))
    ) as create_raw,
    coalesce(
      JSONExtractInt(conv_raw, 'end'),
      toInt64OrNull(JSONExtractString(conv_raw, 'end'))
    ) as end_raw
  from src
  where conv_raw is not null and conv_raw not in ('', 'null', '{}')
),
users as (
  select user_id, any(name) as user_name, any(extension) as user_extension
  from {{ ref('int_pbx__users_current') }}
  final
  where user_id is not null and user_id != ''
  group by user_id
)
select
  p.entry_id,
  p.started_at,
  p.conversion_id,
  p.channel_type,
  p.result_code,
  multiIf(
    p.result_code = 1, 'processed',
    p.result_code = 2, 'transferred',
    p.result_code = 3, 'timeout',
    p.result_code = 4, 'no_answer',
    p.result_code = 5, 'spam',
    p.result_code = 6, 'sending_forbidden',
    toString(p.result_code)
  ) as result_label,
  p.assign_user_id,
  ua.user_name as assign_user_name,
  ua.user_extension as assign_user_extension,
  p.close_user_id,
  uc.user_name as close_user_name,
  uc.user_extension as close_user_extension,
  p.contact_id,
  p.group_id,
  p.deal_id,
  p.entry_point,
  if(isNull(p.first_answer_raw) or p.first_answer_raw <= 0, NULL,
     toDateTime64(if(p.first_answer_raw >= 1000000000000, p.first_answer_raw / 1000.0, toFloat64(p.first_answer_raw)), 3)
  ) as first_answer_at,
  if(isNull(p.taken_raw) or p.taken_raw <= 0, NULL,
     toDateTime64(if(p.taken_raw >= 1000000000000, p.taken_raw / 1000.0, toFloat64(p.taken_raw)), 3)
  ) as taken_at,
  if(isNull(p.create_raw) or p.create_raw <= 0, NULL,
     toDateTime64(if(p.create_raw >= 1000000000000, p.create_raw / 1000.0, toFloat64(p.create_raw)), 3)
  ) as created_at,
  if(isNull(p.end_raw) or p.end_raw <= 0, NULL,
     toDateTime64(if(p.end_raw >= 1000000000000, p.end_raw / 1000.0, toFloat64(p.end_raw)), 3)
  ) as ended_at,
  {{ ch_json_from_string("nullIf(p.conv_raw, '')") }} as conversion_json
from parsed as p
left join users as ua on ua.user_id = p.assign_user_id
left join users as uc on uc.user_id = p.close_user_id
