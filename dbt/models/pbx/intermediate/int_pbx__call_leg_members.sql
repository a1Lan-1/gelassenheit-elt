{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='entry_id',
    alias='cur_call_leg_members',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id, parent_leg_seq, member_seq)',
    settings={'allow_nullable_key': 1},
    tags=['pbx', 'calls', 'legs', 'current'],
  )
}}

-- Group dial-in participants (members[] inside group-legs).
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
legs_raw as (
  select
    c.entry_id,
    c.started_at,
    arrayJoin(JSONExtractArrayRaw(ifNull(c.raw_str, '{}'), 'context_calls')) as leg,
    row_number() over (partition by c.entry_id order by leg) as parent_leg_seq
  from calls as c
),
members_raw as (
  select
    entry_id,
    started_at,
    parent_leg_seq,
    nullIf(JSONExtractString(leg, 'call_type'), '') as parent_call_type,
    nullIf(toString(JSONExtractInt(leg, 'call_abonent_id')), '0') as parent_abonent_id,
    arrayJoin(JSONExtractArrayRaw(leg, 'members')) as member_leg,
    row_number() over (partition by entry_id, parent_leg_seq order by member_leg) as member_seq
  from legs_raw
  where length(JSONExtractArrayRaw(leg, 'members')) > 0
),
members as (
  WITH
    coalesce(
      JSONExtractInt(member_leg, 'call_answer_time'),
      toInt64OrNull(JSONExtractString(member_leg, 'call_answer_time'))
    ) AS answer_raw
  select
    entry_id,
    started_at,
    parent_leg_seq,
    parent_call_type,
    parent_abonent_id,
    member_seq,
    nullIf(JSONExtractString(member_leg, 'external_call_id'), '') as external_call_id,
    nullIf(JSONExtractString(member_leg, 'call_type'), '') as call_type,
    nullIf(toString(JSONExtractInt(member_leg, 'call_abonent_id')), '0') as call_abonent_id,
    nullIf(JSONExtractString(member_leg, 'call_abonent_info'), '') as call_abonent_info,
    nullIf(JSONExtractString(member_leg, 'call_abonent_number'), '') as call_abonent_number,
    nullIf(JSONExtractString(member_leg, 'call_abonent_extension'), '') as call_abonent_extension,
    toInt64(ifNull(JSONExtractInt(member_leg, 'call_end_reason'), 0)) as call_end_reason,
    toUInt8(JSONHas(member_leg, 'call_answer_time') and answer_raw is not null and answer_raw > 0) as is_answered,
    if(isNull(answer_raw) or answer_raw <= 0, NULL, toDateTime64(if(answer_raw >= 1000000000000, answer_raw / 1000.0, toFloat64(answer_raw)), 3)) as leg_answered_at,
    toInt64(ifNull(JSONExtractInt(member_leg, 'talk_duration'), 0)) as talk_duration,
    toInt64(ifNull(JSONExtractInt(member_leg, 'dial_duration'), 0)) as dial_duration
  from members_raw
),
users as (
  select user_id, any(name) as user_name, any(extension) as user_extension
  from {{ ref('int_pbx__users_current') }}
  final
  where user_id is not null and user_id != ''
  group by user_id
)
select
  m.entry_id,
  m.started_at,
  m.parent_leg_seq,
  m.parent_call_type,
  m.parent_abonent_id,
  m.member_seq,
  m.external_call_id,
  m.call_type,
  m.call_abonent_id,
  m.call_abonent_info,
  m.call_abonent_number,
  m.call_abonent_extension,
  m.call_end_reason,
  CAST(m.is_answered, 'Bool') as is_answered,
  m.leg_answered_at,
  m.talk_duration,
  m.dial_duration,
  coalesce(u.user_id, m.call_abonent_id) as resolved_user_id,
  coalesce(u.user_name, m.call_abonent_info) as resolved_user_name,
  coalesce(u.user_extension, m.call_abonent_extension) as resolved_extension
from members as m
left join users as u
  on u.user_id = m.call_abonent_id
