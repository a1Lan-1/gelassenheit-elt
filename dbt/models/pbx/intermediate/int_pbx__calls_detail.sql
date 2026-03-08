{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='entry_id',
    alias='cur_calls_detail',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id, dialog_seq)',
    settings={'allow_nullable_key': 1},
    tags=['pbx', 'calls', 'detail'],
  )
}}

-- Step dialogs: one row per agent dialog (entry_id, dialog_seq).
-- Incremental delete+insert by entry_id for lookback (late legs change dialog_seq).

{% set lookback_days = var('pbx_calls_detail_lookback_days', 1) | int %}
{% set lookback_hours = var('pbx_calls_detail_lookback_hours', 1) | int %}

with calls as (
  select
    entry_id,
    started_at as entry_started_at,
    context_type,
    context_status,
    context_init_type,
    recall_status,
    caller_id,
    caller_name,
    caller_number,
    called_number,
    client_phone,
    call_type_code as entry_call_type_code,
    waiting_on_line_time as entry_waiting_on_line_time,
    waiting_time as entry_waiting_time,
    -- Entry-level talk/duration for outbound_fallback (no answered user leg).
    toInt64(greatest(ifNull(duration, 0), 0)) as entry_duration,
    toInt64(greatest(ifNull(talk_duration, 0), 0)) as entry_talk_duration,
    toInt64(greatest(ifNull(aht_talk_duration, 0), 0)) as entry_aht_talk_duration,
    cost,
    context_cost_full,
    context_cost_tariff,
    recording_ids,
    period,
    toString(raw_data) as raw_str
  from (
    -- No FULL FINAL on history: lookback first, then dedup.
    select *
    from {{ ref('int_pbx__calls_current') }}
    {% if is_incremental() %}
    -- Hourly run: narrow window (late legs); days — buffer for FR/manual override.
    where started_at >= now() - toIntervalHour({{ lookback_hours }})
    {% endif %}
    order by entry_id, started_at desc
    limit 1 by entry_id
  )
),
contexts as (
  select
    entry_id,
    arrayJoin(JSONExtractArrayRaw(ifNull(raw_str, '{}'), 'context_calls')) as context_leg
  from calls
),
leg_flat as (
  select
    entry_id,
    nullIf(JSONExtractString(context_leg, 'call_type'), '') as parent_call_type,
    nullIf(toString(JSONExtractInt(context_leg, 'call_abonent_id')), '0') as parent_abonent_id,
    toUInt8(ifNull(JSONExtractBool(context_leg, 'DirectionInbound'), 0)) as parent_direction_inbound,
    toUInt8(ifNull(JSONExtractBool(context_leg, 'BlindTransfer'), 0)) as parent_blind,
    toUInt8(ifNull(JSONExtractBool(context_leg, 'ConsultTransfer'), 0)) as parent_consult,
    arrayJoin(
      arrayPushFront(JSONExtractArrayRaw(context_leg, 'members'), context_leg)
    ) as leg
  from contexts
),
leg_norm as (
  with
    coalesce(
      JSONExtractInt(leg, 'call_answer_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_answer_time'))
    ) as answer_raw,
    coalesce(
      JSONExtractInt(leg, 'call_start_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_start_time'))
    ) as start_raw,
    coalesce(
      JSONExtractInt(leg, 'call_end_time'),
      toInt64OrNull(JSONExtractString(leg, 'call_end_time'))
    ) as end_raw
  select
    entry_id,
    parent_call_type,
    parent_abonent_id,
    parent_direction_inbound,
    nullIf(JSONExtractString(leg, 'external_call_id'), '') as external_call_id,
    nullIf(JSONExtractString(leg, 'call_type'), '') as call_type,
    nullIf(toString(JSONExtractInt(leg, 'call_abonent_id')), '0') as call_abonent_id,
    nullIf(JSONExtractString(leg, 'call_abonent_info'), '') as call_abonent_info,
    nullIf(JSONExtractString(leg, 'call_abonent_extension'), '') as call_abonent_extension,
    nullIf(JSONExtractString(leg, 'call_abonent_number'), '') as call_abonent_number,
    toUInt8(ifNull(JSONExtractBool(leg, 'DirectionInbound'), 0)) as direction_inbound,
    toUInt8(ifNull(JSONExtractBool(leg, 'DirectionOutbound'), 0)) as direction_outbound,
    greatest(
      toUInt8(ifNull(JSONExtractBool(leg, 'BlindTransfer'), 0)),
      parent_blind
    ) as is_blind_transfer_leg,
    greatest(
      toUInt8(ifNull(JSONExtractBool(leg, 'ConsultTransfer'), 0)),
      parent_consult
    ) as is_consult_transfer_leg,
    toInt64(ifNull(JSONExtractInt(leg, 'call_duration'), 0)) as call_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'talk_duration'), 0)) as talk_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'dial_duration'), 0)) as dial_duration,
    toInt64(ifNull(JSONExtractInt(leg, 'hold_duration'), 0)) as hold_duration,
    toUInt8(
      JSONHas(leg, 'call_answer_time')
      and answer_raw is not null
      and answer_raw > 0
    ) as is_answered,
    if(
      isNull(start_raw) or start_raw <= 0,
      NULL,
      toDateTime64(
        if(start_raw >= 1000000000000, start_raw / 1000.0, toFloat64(start_raw)),
        3
      )
    ) as dialog_started_at,
    if(
      isNull(answer_raw) or answer_raw <= 0,
      NULL,
      toDateTime64(
        if(answer_raw >= 1000000000000, answer_raw / 1000.0, toFloat64(answer_raw)),
        3
      )
    ) as employee_accepted_at,
    if(
      isNull(end_raw) or end_raw <= 0,
      NULL,
      toDateTime64(
        if(end_raw >= 1000000000000, end_raw / 1000.0, toFloat64(end_raw)),
        3
      )
    ) as dialog_ended_at,
    JSONExtractArrayRaw(leg, 'recording_id') as recording_id_raw,
    JSONExtractInt(leg, 'call_end_reason') as call_end_reason
  from leg_flat
),
-- Answered user legs (member preferred over group-parent — parent is not user).
answered_users as (
  select *
  from leg_norm
  where call_type = 'user'
    and is_answered = 1
    and call_abonent_id is not null
    and call_abonent_id != ''
),
answered_dedup as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by
          entry_id,
          call_abonent_id,
          coalesce(toString(employee_accepted_at), ''),
          coalesce(external_call_id, '')
        order by
          talk_duration desc,
          call_duration desc,
          hold_duration desc
      ) as rn
    from answered_users
  )
  where rn = 1
),
entry_flags as (
  select
    entry_id,
    max(direction_inbound) as has_inbound_leg,
    max(direction_outbound) as has_outbound_leg,
    max(is_answered) as has_answered_leg,
    anyIf(
      parent_abonent_id,
      parent_call_type = 'group'
        and parent_direction_inbound = 1
        and parent_abonent_id is not null
        and parent_abonent_id != ''
    ) as leg_group_id,
    -- Dial target on missed (direct-to-user): unanswered inbound user-leg.
    anyIf(
      call_abonent_id,
      call_type = 'user'
        and direction_inbound = 1
        and is_answered = 0
        and call_abonent_id is not null
        and call_abonent_id != ''
    ) as missed_target_user_id,
    anyIf(
      call_abonent_info,
      call_type = 'user'
        and direction_inbound = 1
        and is_answered = 0
        and call_abonent_id is not null
        and call_abonent_id != ''
    ) as missed_target_label,
    anyIf(
      call_abonent_extension,
      call_type = 'user'
        and direction_inbound = 1
        and is_answered = 0
        and call_abonent_id is not null
        and call_abonent_id != ''
    ) as missed_target_extension,
    anyIf(call_abonent_number, direction_inbound = 1) as leg_client_phone_inbound,
    anyIf(call_abonent_number, direction_outbound = 1) as leg_client_phone_outbound,
    arrayDistinct(arrayFlatten(groupArray(recording_id_raw))) as recording_id_items,
    max(call_end_reason) as entry_call_end_reason,
    max(dial_duration) as entry_dial_max,
    max(hold_duration) as entry_hold_max
  from leg_norm
  group by entry_id
),
dialogs_raw as (
  select
    a.entry_id,
    a.call_abonent_id as user_id,
    a.call_abonent_info as answered_label,
    a.call_abonent_extension as answered_extension,
    a.direction_inbound,
    a.direction_outbound,
    a.is_blind_transfer_leg,
    a.is_consult_transfer_leg,
    a.call_duration,
    a.talk_duration,
    a.dial_duration,
    a.hold_duration,
    a.dialog_started_at,
    a.employee_accepted_at,
    a.dialog_ended_at,
    a.call_end_reason,
    a.recording_id_raw,
    a.external_call_id
  from answered_dedup as a
),
dialogs_seq as (
  select
    d.*,
    toUInt32(
      row_number() over (
        partition by d.entry_id
        order by
          coalesce(d.employee_accepted_at, d.dialog_started_at) asc nulls last,
          d.user_id
      )
    ) as dialog_seq
  from dialogs_raw as d
),
dialogs_with_prev as (
  select
    curr.*,
    prev.user_id as prev_user_id,
    prev.dialog_ended_at as prev_ended_at,
    prev.employee_accepted_at as prev_accepted_at,
    prev.is_blind_transfer_leg as prev_blind,
    prev.is_consult_transfer_leg as prev_consult
  from dialogs_seq as curr
  left join dialogs_seq as prev
    on prev.entry_id = curr.entry_id
    and prev.dialog_seq = toUInt32(curr.dialog_seq - 1)
),
-- Synthetic missed: inbound, nobody answered.
-- user_* = dial target (non-answering party) when call went direct to user.
missed_rows as (
  select
    c.entry_id,
    toUInt32(1) as dialog_seq,
    nullIf(ef.missed_target_user_id, '') as user_id,
    nullIf(ef.missed_target_label, '') as answered_label,
    nullIf(ef.missed_target_extension, '') as answered_extension,
    'missed' as call_type_code,
    CAST(1 AS Bool) as missed_flag,
    CAST(0 AS Bool) as is_consultation,
    CAST(0 AS Bool) as is_blind_transfer,
    CAST(0 AS Bool) as is_hold_transfer,
    'none' as transfer_kind,
    CAST(NULL AS Nullable(String)) as transfer_from_user_id,
    CAST(NULL AS Nullable(String)) as transfer_to_user_id,
    CAST(0 AS Bool) as transfer_success,
    CAST(0 AS Bool) as is_real_transfer_hop,
    CAST(0 AS Bool) as is_consult_bridge_leg,
    c.entry_started_at as dialog_started_at,
    CAST(NULL AS Nullable(DateTime64(3))) as employee_accepted_at,
    c.entry_started_at + toIntervalSecond(toUInt32(greatest(toFloat64(ifNull(c.entry_waiting_time, toFloat64(ifNull(ef.entry_dial_max, 0)))), toFloat64(0)))) as dialog_ended_at,
    toFloat64(greatest(toFloat64(ifNull(c.entry_waiting_on_line_time, toFloat64(ifNull(ef.entry_dial_max, 0)))), toFloat64(0))) as waiting_on_line_time,
    toFloat64(greatest(toFloat64(ifNull(c.entry_waiting_time, toFloat64(ifNull(ef.entry_dial_max, 0)))), toFloat64(0))) as waiting_time,
    CAST(NULL AS Nullable(Int64)) as gap_from_prev_sec,
    toFloat64(greatest(toFloat64(ifNull(c.entry_waiting_on_line_time, toFloat64(ifNull(ef.entry_dial_max, 0)))), toFloat64(0))) as wait_to_accept_sec,
    toInt64(greatest(toFloat64(ifNull(c.entry_waiting_time, toFloat64(ifNull(ef.entry_dial_max, 0)))), toFloat64(0))) as duration,
    toInt64(0) as talk_duration,
    toInt64(greatest(ifNull(ef.entry_dial_max, 0), 0)) as dial_duration,
    toInt64(0) as hold_duration,
    toInt64(0) as duration_holding,
    toInt64(greatest(toFloat64(ifNull(c.entry_waiting_time, toFloat64(ifNull(ef.entry_dial_max, 0)))), toFloat64(0))) as duration_without_holding,
    ef.entry_call_end_reason as call_end_reason,
    CAST([] AS Array(String)) as recording_id_raw
  from calls as c
  left join entry_flags as ef on ef.entry_id = c.entry_id
  where ifNull(ef.has_inbound_leg, 0) = 1
    and ifNull(ef.has_answered_leg, 0) = 0
    and c.entry_id not in (select entry_id from dialogs_seq)
),
-- Outbound without answered user leg: operator = caller_id.
-- Talk/duration from entry (cur_calls): in raw often on context parent, not user-member.
outbound_fallback as (
  select
    c.entry_id,
    toUInt32(1) as dialog_seq,
    nullIf(toString(c.caller_id), '') as user_id,
    nullIf(c.caller_name, '') as answered_label,
    CAST(NULL AS Nullable(String)) as answered_extension,
    'outgoing' as call_type_code,
    CAST(0 AS Bool) as missed_flag,
    CAST(0 AS Bool) as is_consultation,
    CAST(0 AS Bool) as is_blind_transfer,
    CAST(0 AS Bool) as is_hold_transfer,
    'none' as transfer_kind,
    CAST(NULL AS Nullable(String)) as transfer_from_user_id,
    CAST(NULL AS Nullable(String)) as transfer_to_user_id,
    CAST(0 AS Bool) as transfer_success,
    CAST(0 AS Bool) as is_real_transfer_hop,
    CAST(0 AS Bool) as is_consult_bridge_leg,
    c.entry_started_at as dialog_started_at,
    c.entry_started_at as employee_accepted_at,
    if(
      c.entry_talk_duration > 0 or c.entry_duration > 0,
      c.entry_started_at
        + toIntervalSecond(
          toUInt32(greatest(c.entry_duration, c.entry_talk_duration, c.entry_aht_talk_duration))
        ),
      CAST(NULL AS Nullable(DateTime64(3)))
    ) as dialog_ended_at,
    toFloat64(0) as waiting_on_line_time,
    toFloat64(ifNull(c.entry_waiting_time, 0)) as waiting_time,
    CAST(NULL AS Nullable(Int64)) as gap_from_prev_sec,
    toFloat64(0) as wait_to_accept_sec,
    toInt64(
      greatest(c.entry_duration, c.entry_talk_duration, c.entry_aht_talk_duration)
    ) as duration,
    toInt64(greatest(c.entry_talk_duration, c.entry_aht_talk_duration)) as talk_duration,
    toInt64(greatest(toInt64(ifNull(c.entry_waiting_time, 0)), 0)) as dial_duration,
    toInt64(0) as hold_duration,
    toInt64(0) as duration_holding,
    toInt64(greatest(c.entry_talk_duration, c.entry_aht_talk_duration)) as duration_without_holding,
    ef.entry_call_end_reason as call_end_reason,
    CAST([] AS Array(String)) as recording_id_raw
  from calls as c
  left join entry_flags as ef on ef.entry_id = c.entry_id
  where ifNull(ef.has_outbound_leg, 0) = 1
    and ifNull(ef.has_inbound_leg, 0) = 0
    and c.entry_id not in (select entry_id from dialogs_seq)
    and c.entry_id not in (select entry_id from missed_rows)
),
dialogs_typed as (
  select
    d.entry_id,
    toUInt32(d.dialog_seq) as dialog_seq,
    d.user_id,
    d.answered_label,
    d.answered_extension,
    multiIf(
      d.dialog_seq = 1 and ifNull(d.direction_outbound, 0) = 1 and ifNull(d.direction_inbound, 0) = 0,
        'outgoing',
      d.dialog_seq = 1,
        'incoming',
      -- Later stages: consult separate from blind/transfered.
      d.is_consult_transfer_leg = 1 or ifNull(d.prev_consult, 0) = 1,
        'consult',
      d.is_blind_transfer_leg = 1
        or ifNull(d.prev_blind, 0) = 1
        or (
          d.prev_user_id is not null
          and d.prev_user_id != ''
          and d.prev_user_id != d.user_id
        ),
        'transfered',
      'transfered'
    ) as call_type_code,
    CAST(0 AS Bool) as missed_flag,
    CAST(
      d.dialog_seq > 1
        and (d.is_consult_transfer_leg = 1 or ifNull(d.prev_consult, 0) = 1),
      'Bool'
    ) as is_consultation,
    CAST(
      d.dialog_seq > 1
        and (d.is_blind_transfer_leg = 1 or ifNull(d.prev_blind, 0) = 1)
        and not (d.is_consult_transfer_leg = 1 or ifNull(d.prev_consult, 0) = 1),
      'Bool'
    ) as is_blind_transfer,
    -- Hold — stage attribute, not call type.
    CAST(ifNull(d.hold_duration, 0) > 0, 'Bool') as is_hold_transfer,
    multiIf(
      d.dialog_seq = 1, 'none',
      d.is_consult_transfer_leg = 1 or ifNull(d.prev_consult, 0) = 1, 'consult',
      d.is_blind_transfer_leg = 1 or ifNull(d.prev_blind, 0) = 1, 'blind',
      'other_transfer'
    ) as transfer_kind,
    if(d.dialog_seq > 1, d.prev_user_id, CAST(NULL AS Nullable(String))) as transfer_from_user_id,
    if(d.dialog_seq > 1, d.user_id, CAST(NULL AS Nullable(String))) as transfer_to_user_id,
    CAST(
      d.dialog_seq > 1
        and d.prev_user_id is not null
        and d.prev_user_id != ''
        and d.user_id is not null
        and d.user_id != ''
        and d.prev_user_id != d.user_id,
      'Bool'
    ) as is_real_transfer_hop,
    CAST(
      d.dialog_seq > 1
        and not (
          d.prev_user_id is not null
          and d.prev_user_id != ''
          and d.user_id is not null
          and d.user_id != ''
          and d.prev_user_id != d.user_id
        ),
      'Bool'
    ) as is_consult_bridge_leg,
    CAST(
      d.dialog_seq > 1
        and d.prev_user_id is not null
        and d.prev_user_id != ''
        and d.user_id is not null
        and d.user_id != ''
        and d.prev_user_id != d.user_id
        and ifNull(d.talk_duration, 0) > 0,
      'Bool'
    ) as transfer_success,
    d.dialog_started_at,
    d.employee_accepted_at,
    d.dialog_ended_at,
    -- Wait until stage accept.
    -- hop1: dial on user-leg in group-hunt = (start→end)−talk, not ring; only start→accept.
    -- seq>1: max(dial, start→accept, max(0, prev_end→accept)); consult overlap → dial.
    if(
      d.dialog_seq = 1,
      toFloat64(
        if(
          d.employee_accepted_at is not null and d.dialog_started_at is not null,
          greatest(dateDiff('second', d.dialog_started_at, d.employee_accepted_at), 0),
          ifNull(d.dial_duration, 0)
        )
      ),
      toFloat64(
        greatest(
          ifNull(d.dial_duration, 0),
          if(
            d.employee_accepted_at is not null and d.dialog_started_at is not null,
            greatest(dateDiff('second', d.dialog_started_at, d.employee_accepted_at), 0),
            0
          ),
          if(
            d.employee_accepted_at is not null
              and coalesce(d.prev_ended_at, d.prev_accepted_at) is not null,
            greatest(
              dateDiff(
                'second',
                coalesce(d.prev_ended_at, d.prev_accepted_at),
                d.employee_accepted_at
              ),
              0
            ),
            0
          )
        )
      )
    ) as waiting_on_line_time,
    toFloat64(greatest(ifNull(d.dial_duration, 0), 0)) as waiting_time,
    if(
      d.dialog_seq = 1,
      CAST(NULL AS Nullable(Int64)),
      toInt64(
        greatest(
          ifNull(d.dial_duration, 0),
          if(
            d.employee_accepted_at is not null and d.dialog_started_at is not null,
            greatest(dateDiff('second', d.dialog_started_at, d.employee_accepted_at), 0),
            0
          ),
          if(
            d.employee_accepted_at is not null
              and coalesce(d.prev_ended_at, d.prev_accepted_at) is not null,
            greatest(
              dateDiff(
                'second',
                coalesce(d.prev_ended_at, d.prev_accepted_at),
                d.employee_accepted_at
              ),
              0
            ),
            0
          )
        )
      )
    ) as gap_from_prev_sec,
    if(
      d.dialog_seq = 1,
      toFloat64(
        if(
          d.employee_accepted_at is not null and d.dialog_started_at is not null,
          greatest(dateDiff('second', d.dialog_started_at, d.employee_accepted_at), 0),
          ifNull(d.dial_duration, 0)
        )
      ),
      toFloat64(
        greatest(
          ifNull(d.dial_duration, 0),
          if(
            d.employee_accepted_at is not null and d.dialog_started_at is not null,
            greatest(dateDiff('second', d.dialog_started_at, d.employee_accepted_at), 0),
            0
          ),
          if(
            d.employee_accepted_at is not null
              and coalesce(d.prev_ended_at, d.prev_accepted_at) is not null,
            greatest(
              dateDiff(
                'second',
                coalesce(d.prev_ended_at, d.prev_accepted_at),
                d.employee_accepted_at
              ),
              0
            ),
            0
          )
        )
      )
    ) as wait_to_accept_sec,
    toInt64(
      greatest(
        ifNull(d.call_duration, 0),
        ifNull(d.talk_duration, 0),
        if(
          d.dialog_ended_at is not null and d.dialog_started_at is not null,
          greatest(dateDiff('second', d.dialog_started_at, d.dialog_ended_at), 0),
          0
        )
      )
    ) as duration,
    toInt64(greatest(ifNull(d.talk_duration, 0), 0)) as talk_duration,
    toInt64(greatest(ifNull(d.dial_duration, 0), 0)) as dial_duration,
    toInt64(greatest(ifNull(d.hold_duration, 0), 0)) as hold_duration,
    toInt64(greatest(ifNull(d.hold_duration, 0), 0)) as duration_holding,
    toInt64(
      greatest(
        greatest(ifNull(d.call_duration, 0), ifNull(d.talk_duration, 0))
          - ifNull(d.hold_duration, 0),
        0
      )
    ) as duration_without_holding,
    d.call_end_reason,
    d.recording_id_raw
  from dialogs_with_prev as d
),
all_dialogs_raw as (
  select * from dialogs_typed
  union all
  select * from missed_rows
  union all
  select * from outbound_fallback
),
-- AHT unit: consult-bridge not counted separately; talk merged with previous real hop.
all_dialogs as (
  select
    d.*,
    if(
      d.is_consult_bridge_leg,
      CAST(NULL AS Nullable(Int64)),
      toInt64(
        greatest(ifNull(d.talk_duration, 0), 0)
        + if(
          leadInFrame(d.is_consult_bridge_leg, 1) over w = 1
            and leadInFrame(d.user_id, 1) over w = d.user_id
            and d.user_id is not null
            and d.user_id != '',
          greatest(ifNull(leadInFrame(d.talk_duration, 1) over w, 0), 0),
          0
        )
      )
    ) as aht_talk_duration
  from all_dialogs_raw as d
  window w as (
    partition by d.entry_id
    order by d.dialog_seq
    rows between unbounded preceding and unbounded following
  )
),
conv as (
  select
    entry_id,
    nullIf(JSONExtractRaw(ifNull(raw_str, '{}'), 'conversion'), '') as conv_raw
  from calls
),
conv_parsed as (
  select
    entry_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'assign_user_id')), '0') as assign_user_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'close_user_id')), '0') as close_user_id,
    JSONExtractInt(conv_raw, 'result') as result_code,
    nullIf(toString(JSONExtractInt(conv_raw, 'group_id')), '0') as conversion_group_id,
    nullIf(toString(JSONExtractInt(conv_raw, 'deal_id')), '0') as deal_id,
    nullIf(JSONExtractString(conv_raw, 'entry_point'), '') as entry_point,
    nullIf(toString(JSONExtractInt(conv_raw, 'contact_id')), '0') as contact_id,
    conv_raw
  from conv
  where conv_raw is not null and conv_raw not in ('', 'null', '{}')
),
users as (
  select
    user_id,
    any(name) as user_name,
    any(extension) as user_extension,
    any(email) as user_email,
    any(department) as user_department,
    any(position) as user_position,
    any(
      nullIf(
        toString(JSONExtractInt(toString(groups), 'items', 1)),
        '0'
      )
    ) as user_group_id
  from {{ ref('int_pbx__users_current') }}
  final
  where user_id is not null and user_id != ''
  group by user_id
),
groups as (
  select
    group_id,
    any(name) as group_name,
    any(extension) as group_extension
  from {{ ref('int_pbx__groups_current') }}
  final
  where group_id is not null and group_id != ''
  group by group_id
),
base as (
  select
    d.entry_id as entry_id,
    d.dialog_seq as dialog_seq,
    c.entry_started_at as entry_started_at,
    coalesce(d.dialog_started_at, d.employee_accepted_at, c.entry_started_at) as started_at,
    d.dialog_started_at as dialog_started_at,
    d.employee_accepted_at as employee_accepted_at,
    d.dialog_ended_at as dialog_ended_at,
    d.user_id as user_id,
    d.answered_label as answered_label,
    d.answered_extension as answered_extension,
    d.call_type_code as call_type_code,
    d.missed_flag as missed_flag,
    d.is_consultation as is_consultation,
    d.is_blind_transfer as is_blind_transfer,
    d.is_hold_transfer as is_hold_transfer,
    d.transfer_kind as transfer_kind,
    d.transfer_from_user_id as transfer_from_user_id,
    d.transfer_to_user_id as transfer_to_user_id,
    d.transfer_success as transfer_success,
    d.is_real_transfer_hop as is_real_transfer_hop,
    d.is_consult_bridge_leg as is_consult_bridge_leg,
    d.waiting_on_line_time as waiting_on_line_time,
    d.waiting_time as waiting_time,
    d.gap_from_prev_sec as gap_from_prev_sec,
    d.wait_to_accept_sec as wait_to_accept_sec,
    -- ASA: queue → answer. hop1: entry→accept; transfer: wait_to_accept; no answer — NULL.
    if(
      d.employee_accepted_at is not null,
      if(
        d.dialog_seq = 1,
        toFloat64(greatest(dateDiff('second', c.entry_started_at, d.employee_accepted_at), 0)),
        toFloat64(d.wait_to_accept_sec)
      ),
      CAST(NULL AS Nullable(Float64))
    ) as asa_sec,
    d.duration as duration,
    d.talk_duration as talk_duration,
    d.aht_talk_duration as aht_talk_duration,
    d.dial_duration as dial_duration,
    d.hold_duration as hold_duration,
    d.duration_holding as duration_holding,
    d.duration_without_holding as duration_without_holding,
    -- assigned ≈ stage wait start.
    if(
      d.employee_accepted_at is not null,
      d.employee_accepted_at - toIntervalSecond(toUInt32(greatest(toFloat64(d.waiting_on_line_time), toFloat64(0)))),
      d.dialog_started_at
    ) as employee_assigned_at,
    replaceRegexpAll(
      if(
        d.call_type_code = 'outgoing'
          or (
            ifNull(ef.has_outbound_leg, 0) = 1
            and ifNull(ef.has_inbound_leg, 0) = 0
          ),
        coalesce(nullIf(c.called_number, ''), nullIf(c.client_phone, ''), nullIf(ef.leg_client_phone_outbound, ''), ''),
        coalesce(nullIf(c.caller_number, ''), nullIf(c.client_phone, ''), nullIf(ef.leg_client_phone_inbound, ''), '')
      ),
      '[^0-9]',
      ''
    ) as client_phone,
    c.caller_id as caller_id,
    c.caller_name as caller_name,
    c.caller_number as caller_number,
    c.called_number as called_number,
    c.context_type as context_type,
    c.context_status as context_status,
    c.context_init_type as context_init_type,
    c.recall_status as recall_status,
    -- Session entry queue (conversion / inbound group-leg), not operator group.
    coalesce(nullIf(cp.conversion_group_id, ''), nullIf(ef.leg_group_id, '')) as queue_group_id,
    -- Raw inbound group-leg (for sz-*: line group may ≠ queue).
    nullIf(ef.leg_group_id, '') as leg_group_id,
    cp.assign_user_id as assign_user_id,
    cp.close_user_id as close_user_id,
    cp.result_code as result_code,
    multiIf(
      cp.result_code = 1, 'processed',
      cp.result_code = 2, 'transferred',
      cp.result_code = 3, 'timeout',
      cp.result_code = 4, 'no_answer',
      cp.result_code = 5, 'spam',
      cp.result_code = 6, 'sending_forbidden',
      if(cp.result_code is null, CAST(NULL AS Nullable(String)), toString(cp.result_code))
    ) as result_label,
    cp.deal_id as deal_id,
    cp.entry_point as entry_point,
    cp.contact_id as contact_id,
    nullIf(JSONExtractString(ifNull(c.raw_str, '{}'), 'call_comment'), '') as call_comment,
    -- Scores from ext_params=1 (post-call survey / controller); missing key → NULL.
    JSONExtract(ifNull(c.raw_str, '{}'), 'mark_client', 'Nullable(Int64)') as mark_client,
    JSONExtract(ifNull(c.raw_str, '{}'), 'mark_controller', 'Nullable(Int64)') as mark_controller,
    {{ ch_json_from_string("nullIf(JSONExtractRaw(ifNull(c.raw_str, '{}'), 'tag_id'), '')") }} as tag_ids,
    {{ ch_json_from_string("nullIf(cp.conv_raw, '')") }} as conversion,
    c.cost as cost,
    c.context_cost_full as context_cost_full,
    c.context_cost_tariff as context_cost_tariff,
    coalesce(
      {{ ch_json_from_string("if(length(d.recording_id_raw) > 0, concat('[', arrayStringConcat(d.recording_id_raw, ','), ']'), CAST(NULL AS Nullable(String)))") }},
      {{ ch_json_from_string("if(length(ef.recording_id_items) > 0, concat('[', arrayStringConcat(ef.recording_id_items, ','), ']'), CAST(NULL AS Nullable(String)))") }},
      c.recording_ids
    ) as recording_ids,
    coalesce(d.call_end_reason, ef.entry_call_end_reason) as call_end_reason,
    c.period as period,
    c.raw_str as raw_str
  from all_dialogs as d
  inner join calls as c on c.entry_id = d.entry_id
  left join entry_flags as ef on ef.entry_id = d.entry_id
  left join conv_parsed as cp on cp.entry_id = d.entry_id
)
select
  b.entry_id as entry_id,
  b.dialog_seq as dialog_seq,
  b.entry_started_at as entry_started_at,
  b.started_at as started_at,
  b.dialog_started_at as dialog_started_at,
  b.employee_accepted_at as employee_accepted_at,
  b.dialog_ended_at as dialog_ended_at,
  if(
    match(coalesce(nullIf(u.user_name, ''), nullIf(b.answered_label, ''), ''), '^[0-9+ ()-]+$'),
    CAST(NULL AS Nullable(String)),
    b.user_id
  ) as user_id,
  if(
    match(coalesce(nullIf(u.user_name, ''), nullIf(b.answered_label, ''), ''), '^[0-9+ ()-]+$'),
    CAST(NULL AS Nullable(String)),
    coalesce(nullIf(u.user_name, ''), nullIf(b.answered_label, ''))
  ) as user_name,
  if(
    match(coalesce(nullIf(u.user_name, ''), nullIf(b.answered_label, ''), ''), '^[0-9+ ()-]+$'),
    CAST(NULL AS Nullable(String)),
    coalesce(nullIf(u.user_extension, ''), b.answered_extension)
  ) as user_extension,
  if(match(ifNull(u.user_name, ''), '^[0-9+ ()-]+$'), NULL, u.user_email) as user_email,
  if(match(ifNull(u.user_name, ''), '^[0-9+ ()-]+$'), NULL, u.user_department) as user_department,
  if(match(ifNull(u.user_name, ''), '^[0-9+ ()-]+$'), NULL, u.user_position) as user_position,
  -- group_* = responsible group / missed target; else entry queue.
  -- sz-*: leg_group may differ from queue (conversion = entry queue,
  -- leg = line group); standard pbx behavior unchanged.
  coalesce(
    nullIf(u.user_group_id, ''),
    if(
      startsWith(b.entry_id, 'sz-')
        and nullIf(b.leg_group_id, '') is not null
        and nullIf(b.leg_group_id, '') != nullIf(b.queue_group_id, ''),
      nullIf(b.leg_group_id, ''),
      CAST(NULL AS Nullable(String))
    ),
    nullIf(b.queue_group_id, '')
  ) as group_id,
  go.group_name as group_name,
  go.group_extension as group_extension,
  nullIf(b.queue_group_id, '') as queue_group_id,
  gq.group_name as queue_group_name,
  b.call_type_code as call_type_code,
  b.missed_flag as missed_flag,
  b.is_consultation as is_consultation,
  b.is_blind_transfer as is_blind_transfer,
  b.is_hold_transfer as is_hold_transfer,
  b.transfer_kind as transfer_kind,
  b.duration as duration,
  b.aht_talk_duration as aht_talk_duration,
  b.waiting_on_line_time as waiting_on_line_time,
  b.waiting_time as waiting_time,
  b.gap_from_prev_sec as gap_from_prev_sec,
  b.wait_to_accept_sec as wait_to_accept_sec,
  b.asa_sec as asa_sec,
  b.is_real_transfer_hop as is_real_transfer_hop,
  b.is_consult_bridge_leg as is_consult_bridge_leg,
  b.employee_assigned_at as employee_assigned_at,
  b.dial_duration as dial_duration,
  b.hold_duration as hold_duration,
  b.duration_holding as duration_holding,
  b.duration_without_holding as duration_without_holding,
  b.talk_duration as talk_duration,
  b.client_phone as client_phone,
  b.caller_id as caller_id,
  b.caller_name as caller_name,
  b.caller_number as caller_number,
  b.called_number as called_number,
  b.context_type as context_type,
  b.context_status as context_status,
  b.context_init_type as context_init_type,
  b.recall_status as recall_status,
  b.transfer_from_user_id as transfer_from_user_id,
  uf.user_name as transfer_from_user_name,
  nullIf(uf.user_group_id, '') as transfer_from_group_id,
  guf.group_name as transfer_from_group_name,
  b.transfer_to_user_id as transfer_to_user_id,
  ut.user_name as transfer_to_user_name,
  b.transfer_success as transfer_success,
  b.assign_user_id as assign_user_id,
  ua.user_name as assign_user_name,
  b.close_user_id as close_user_id,
  uc.user_name as close_user_name,
  b.result_code as result_code,
  b.result_label as result_label,
  b.deal_id as deal_id,
  b.entry_point as entry_point,
  b.contact_id as contact_id,
  b.call_comment as call_comment,
  b.mark_client as mark_client,
  b.mark_controller as mark_controller,
  b.tag_ids as tag_ids,
  b.cost as cost,
  b.context_cost_full as context_cost_full,
  b.context_cost_tariff as context_cost_tariff,
  b.recording_ids as recording_ids,
  b.call_end_reason as call_end_reason,
  b.period as period,
  b.conversion as conversion
from base as b
left join users as u on u.user_id = b.user_id
left join users as uf on uf.user_id = b.transfer_from_user_id
left join users as ut on ut.user_id = b.transfer_to_user_id
left join users as ua on ua.user_id = b.assign_user_id
left join users as uc on uc.user_id = b.close_user_id
left join groups as go
  on go.group_id = coalesce(
    nullIf(u.user_group_id, ''),
    if(
      startsWith(b.entry_id, 'sz-')
        and nullIf(b.leg_group_id, '') is not null
        and nullIf(b.leg_group_id, '') != nullIf(b.queue_group_id, ''),
      nullIf(b.leg_group_id, ''),
      CAST(NULL AS Nullable(String))
    ),
    nullIf(b.queue_group_id, '')
  )
left join groups as gq
  on gq.group_id = nullIf(b.queue_group_id, '')
left join groups as guf
  on guf.group_id = nullIf(uf.user_group_id, '')
where ifNull(b.waiting_on_line_time, 0) >= 0
