{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='entry_id',
    alias='cur_call_legs',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id, leg_seq)',
    settings={'allow_nullable_key': 1},
    tags=['pbx', 'calls', 'legs', 'current'],
  )
}}

-- Call legs from raw_data.context_calls (+ join employees / groups).
-- Increment: delete+insert by entry_id for lookback hours.
{% set lookback_hours = var('pbx_legs_lookback_hours', 6) | int %}

-- SIP is not joinable: cur_sip may be missing before successful ingest_pbx_sip.
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
legs_raw as (
  select
    c.entry_id,
    c.started_at,
    arrayJoin(JSONExtractArrayRaw(ifNull(c.raw_str, '{}'), 'context_calls')) as leg,
    row_number() over (partition by c.entry_id order by leg) as leg_seq
  from calls as c
),
legs as (
  WITH
    coalesce(
      JSONExtractInt(leg, 'call_answer_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_answer_time'))
    ) AS answer_raw,
    coalesce(
      JSONExtractInt(leg, 'call_start_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_start_time'))
    ) AS start_raw,
    coalesce(
      JSONExtractInt(leg, 'call_end_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_end_time'))
    ) AS end_raw
  select
    entry_id,
    started_at,
    leg_seq,
    nullIf(JSONExtractString(leg, 'external_call_id'), '') as external_call_id,
    nullIf(JSONExtractString(leg, 'call_type'), '') as call_type,
    nullIf(toString(JSONExtractInt(leg, 'call_abonent_id')), '0') as call_abonent_id,
    nullIf(JSONExtractString(leg, 'call_abonent_info'), '') as call_abonent_info,
    nullIf(JSONExtractString(leg, 'call_abonent_number'), '') as call_abonent_number,
    nullIf(JSONExtractString(leg, 'call_abonent_extension'), '') as call_abonent_extension,
    toUInt8(ifNull(JSONExtractBool(leg, 'DirectionInbound'), 0)) as direction_inbound,
    toUInt8(ifNull(JSONExtractBool(leg, 'DirectionOutbound'), 0)) as direction_outbound,
    toUInt8(ifNull(JSONExtractBool(leg, 'BlindTransfer'), 0)) as is_blind_transfer,
    toUInt8(ifNull(JSONExtractBool(leg, 'ConsultTransfer'), 0)) as is_consult_transfer,
    toUInt8(ifNull(JSONExtractBool(leg, 'ModeGroup'), 0)) as is_mode_group,
    toUInt8(ifNull(JSONExtractBool(leg, 'ModeConversation'), 0)) as is_mode_conversation,
    toUInt8(ifNull(JSONExtractBool(leg, 'Intercepted'), 0)) as is_intercepted,
    toUInt8(ifNull(JSONExtractBool(leg, 'IvrNotUsed'), 0)) as ivr_not_used,
    toInt64(ifNull(JSONExtractInt(leg, 'call_duration'), 0)) as call_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'talk_duration'), 0)) as talk_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'dial_duration'), 0)) as dial_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'hold_duration'), 0)) as hold_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'call_end_reason'), 0)) as call_end_reason,
    toUInt8(JSONHas(leg, 'call_answer_time') and answer_raw is not null and answer_raw > 0) as is_answered,
    if(isNull(start_raw) or start_raw <= 0, NULL, toDateTime64(if(start_raw >= 1000000000000, start_raw / 1000.0, toFloat64(start_raw)), 3)) as leg_start_at,
    if(isNull(answer_raw) or answer_raw <= 0, NULL, toDateTime64(if(answer_raw >= 1000000000000, answer_raw / 1000.0, toFloat64(answer_raw)), 3)) as leg_answered_at,
    if(isNull(end_raw) or end_raw <= 0, NULL, toDateTime64(if(end_raw >= 1000000000000, end_raw / 1000.0, toFloat64(end_raw)), 3)) as leg_end_at,
    length(JSONExtractArrayRaw(leg, 'members')) as members_count,
    {{ ch_json_from_string("nullIf(leg, '')") }} as leg_json
  from legs_raw
),
users as (
  select user_id, any(name) as user_name, any(extension) as user_extension
  from {{ ref('int_pbx__users_current') }}
  final
  where user_id is not null and user_id != ''
  group by user_id
),
groups as (
  select group_id, any(name) as group_name, any(extension) as group_extension
  from {{ ref('int_pbx__groups_current') }}
  final
  where group_id is not null and group_id != ''
  group by group_id
)
-- SIP is not hard: cur_sip may not yet be (ingest_pbx_sip).
select
  l.entry_id,
  l.started_at,
  l.leg_seq,
  l.external_call_id,
  l.call_type,
  l.call_abonent_id,
  l.call_abonent_info,
  l.call_abonent_number,
  l.call_abonent_extension,
  l.direction_inbound,
  l.direction_outbound,
  l.is_blind_transfer,
  l.is_consult_transfer,
  l.is_mode_group,
  l.is_mode_conversation,
  l.is_intercepted,
  l.ivr_not_used,
  l.call_duration,
  l.talk_duration,
  l.dial_duration,
  l.hold_duration,
  l.call_end_reason,
  CAST(l.is_answered, 'Bool') as is_answered,
  l.leg_start_at,
  l.leg_answered_at,
  l.leg_end_at,
  l.members_count,
  multiIf(
    l.call_type = 'user', u.user_id,
    NULL
  ) as resolved_user_id,
  multiIf(
    l.call_type = 'user', coalesce(u.user_name, l.call_abonent_info),
    l.call_type = 'group', coalesce(g.group_name, l.call_abonent_info),
    l.call_abonent_info
  ) as resolved_abonent_name,
  multiIf(
    l.call_type = 'user', coalesce(u.user_extension, l.call_abonent_extension),
    l.call_type = 'group', coalesce(g.group_extension, l.call_abonent_extension),
    l.call_abonent_extension
  ) as resolved_extension,
  l.leg_json
from legs as l
left join users as u
  on l.call_type = 'user' and u.user_id = l.call_abonent_id
left join groups as g
  on l.call_type = 'group' and g.group_id = l.call_abonent_id
