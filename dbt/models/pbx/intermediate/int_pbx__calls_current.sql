{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_calls',
    engine=ch_engine_replacing('started_at'),
    order_by='(entry_id)',
    unique_key=['entry_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'calls', 'current'],
  )
}}

-- Stable contract cur_calls: 30 columns matching the existing rmt.
-- Attribution / conversion / tags / marks are only calculated in cur_calls_detail from raw_data.

with raw_source as (
  select `entry_id`, `started_at`, `context_type`, `context_status`, `caller_id`, `caller_name`, `caller_number`, `called_number`, `duration`, `talk_duration`, `context_init_type`, `recall_status`, `cost`, `context_cost_full`, `context_cost_tariff`, `recording_ids`, `period`, `raw_data`
  from {{ bronze_parquet('pbx', 'calls', '`entry_id` Nullable(String), `started_at` String, `context_type` Nullable(String), `context_status` Nullable(String), `caller_id` Nullable(String), `caller_name` Nullable(String), `caller_number` Nullable(String), `called_number` Nullable(String), `duration` Nullable(Int64), `talk_duration` Nullable(Int64), `context_init_type` Nullable(String), `recall_status` Nullable(String), `cost` Nullable(String), `context_cost_full` Nullable(String), `context_cost_tariff` Nullable(String), `recording_ids` Nullable(String), `period` Nullable(String), `raw_data` Nullable(String)') }}
),
typed as (
  select
    assumeNotNull(CAST(`entry_id`, 'Nullable(String)')) AS `entry_id`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `started_at`,
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
    CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`cost`)), ''), ',', '.')), 'Nullable(Float64)') AS `cost`,
    CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`context_cost_full`)), ''), ',', '.')), 'Nullable(Float64)') AS `context_cost_full`,
    CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`context_cost_tariff`)), ''), ',', '.')), 'Nullable(Float64)') AS `context_cost_tariff`,
    CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`recording_ids`)), '')") }}, 'Nullable(JSON)') AS `recording_ids`,
    CAST(`period`, 'Nullable(String)') AS `period`,
    CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
  from raw_source
),
legs as (
  select
    t.entry_id,
    t.started_at,
    arrayJoin(JSONExtractArrayRaw(ifNull(toString(t.raw_data), '{}'), 'context_calls')) as leg
  from typed as t
),
leg_norm as (
  WITH
    coalesce(
      JSONExtractInt(leg, 'call_answer_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_answer_time'))
    ) AS leg_call_answer_time_raw
  select
    entry_id,
    started_at,
    leg as leg_raw,
    nullIf(JSONExtractString(leg, 'external_call_id'), '') as external_call_id,
    toUInt8(ifNull(JSONExtractBool(leg, 'DirectionInbound'), 0)) as direction_inbound,
    toUInt8(ifNull(JSONExtractBool(leg, 'DirectionOutbound'), 0)) as direction_outbound,
    toUInt8(ifNull(JSONExtractBool(leg, 'BlindTransfer'), 0)) as is_blind_transfer_leg,
    toUInt8(ifNull(JSONExtractBool(leg, 'ConsultTransfer'), 0)) as is_consult_transfer_leg,
    toInt64(ifNull(JSONExtractInt(leg, 'call_duration'), 0)) as leg_call_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'talk_duration'), 0)) as leg_talk_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'dial_duration'), 0)) as leg_dial_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'hold_duration'), 0)) as leg_hold_duration,
    toUInt8(JSONHas(leg, 'call_answer_time')) as has_answer_time,
    if(
      isNull(leg_call_answer_time_raw) OR leg_call_answer_time_raw <= 0,
      NULL,
      toDateTime64(
        if(
          leg_call_answer_time_raw >= 1000000000000,
          leg_call_answer_time_raw / 1000.0,
          toFloat64(leg_call_answer_time_raw)
        ),
        3
      )
    ) as leg_employee_accepted_at,
    nullIf(JSONExtractString(leg, 'call_abonent_number'), '') as leg_abonent_number
  from legs
),
leg_dedup as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by
          entry_id,
          coalesce(external_call_id, ''),
          direction_inbound,
          direction_outbound,
          is_blind_transfer_leg,
          is_consult_transfer_leg,
          leg_dial_duration,
          leg_hold_duration,
          leg_call_duration,
          leg_talk_duration,
          has_answer_time,
          coalesce(toString(leg_employee_accepted_at), ''),
          coalesce(leg_abonent_number, '')
        order by
          has_answer_time desc,
          leg_employee_accepted_at desc nulls last,
          leg_talk_duration desc,
          leg_call_duration desc,
          leg_raw desc
      ) as rn
    from leg_norm
  )
  where rn = 1
),
agg as (
  select
    entry_id,
    max(direction_inbound) as has_inbound_leg,
    max(direction_outbound) as has_outbound_leg,
    max(is_blind_transfer_leg) as is_blind_transfer,
    max(is_consult_transfer_leg) as is_consultation,
    max(leg_call_duration) as call_duration_leg_max,
    max(leg_talk_duration) as talk_duration_leg_max,
    max(leg_dial_duration) as derived_waiting_time,
    max(leg_hold_duration) as derived_duration_holding,
    -- Do not use "any unanswered leg": the group-parent often does not have answer_time,
    - even when the member/operator responded.
    max(leg_employee_accepted_at) as employee_accepted_at,
    anyIf(leg_abonent_number, direction_inbound = 1) as client_phone_inbound,
    anyIf(leg_abonent_number, direction_outbound = 1) as client_phone_outbound
  from leg_dedup
  group by entry_id
),
final as (
  select
    t.entry_id,
    t.started_at,
    t.context_type,
    t.context_status,
    t.caller_id,
    t.caller_name,
    t.caller_number,
    t.called_number,
    t.duration,
    t.talk_duration,
    t.context_init_type,
    t.recall_status,
    t.cost,
    t.context_cost_full,
    t.context_cost_tariff,
    - Priority: translation → answered inbound → missed → outbound.
    multiIf(
      ifNull(a.is_blind_transfer, 0) = 1
        or ifNull(a.is_consultation, 0) = 1
        or ifNull(a.derived_duration_holding, 0) > 0,
        'transfered',
      ifNull(a.has_inbound_leg, 0) = 1 and a.employee_accepted_at is not null,
        'incoming',
      ifNull(a.has_inbound_leg, 0) = 1,
        'missed',
      ifNull(a.has_outbound_leg, 0) = 1,
        'outgoing',
      'other'
    ) as call_type_code,
    CAST(
      ifNull(a.has_inbound_leg, 0) = 1
        and a.employee_accepted_at is null
        and ifNull(a.is_blind_transfer, 0) = 0
        and ifNull(a.is_consultation, 0) = 0
        and ifNull(a.derived_duration_holding, 0) = 0,
      'Bool'
    ) as missed_flag,
    CAST(ifNull(a.is_consultation, 0) = 1, 'Bool') as is_consultation,
    CAST(ifNull(a.is_blind_transfer, 0) = 1, 'Bool') as is_blind_transfer,
    replaceRegexpAll(
      multiIf(
        ifNull(a.has_outbound_leg, 0) = 1 and ifNull(a.has_inbound_leg, 0) = 0,
          coalesce(nullIf(t.called_number, ''), nullIf(a.client_phone_outbound, ''), ''),
        ifNull(a.has_inbound_leg, 0) = 1,
          coalesce(nullIf(t.caller_number, ''), nullIf(a.client_phone_inbound, ''), ''),
        ''
      ),
      '[^0-9]',
      ''
    ) as client_phone,
    greatest(ifNull(t.talk_duration, 0), ifNull(a.talk_duration_leg_max, 0)) as aht_talk_duration,
    greatest(ifNull(a.derived_waiting_time, 0), 0) as waiting_time,
    (t.started_at + toIntervalSecond(greatest(ifNull(a.derived_waiting_time, 0), 0))) as employee_assigned_at,
    greatest(greatest(ifNull(a.derived_waiting_time, 0), 0) - ifNull(a.derived_duration_holding, 0), 0) as waiting_on_line_time,
    a.employee_accepted_at as employee_accepted_at,
    a.derived_duration_holding as duration_holding,
    greatest(ifNull(a.call_duration_leg_max, 0) - ifNull(a.derived_duration_holding, 0), 0) as duration_without_holding,
    t.recording_ids,
    t.period,
    t.raw_data
  from typed as t
  left join agg as a
    on a.entry_id = t.entry_id
)
select *
from final
where waiting_on_line_time is null
   or waiting_on_line_time >= 0
