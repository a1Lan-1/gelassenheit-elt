{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='entry_id',
    alias='cur_call_events',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id, event_seq)',
    settings={'allow_nullable_key': 1},
    tags=['pbx', 'calls', 'events'],
  )
}}

-- Dialer-like grain: logical events of the session (incoming / transfer / missed), without consult-bridge hop.
-- Increment: delete+insert by entry_id for lookback.

{% set lookback_days = var('pbx_calls_detail_lookback_days', 1) | int %}
{% set lookback_hours = var('pbx_calls_detail_lookback_hours', 1) | int %}

with raw_events as (
  select
    entry_id,
    dialog_seq,
    started_at,
    entry_started_at,
    call_type_code,
    user_id,
    user_name,
    group_id,
    group_name,
    queue_group_id,
    queue_group_name,
    client_phone,
    wait_to_accept_sec,
    talk_duration,
    transfer_kind,
    transfer_from_user_id,
    transfer_from_user_name,
    transfer_from_group_id,
    transfer_from_group_name,
    transfer_to_user_id,
    transfer_to_user_name,
    transfer_success,
    is_real_transfer_hop
  from {{ ref('int_pbx__calls_detail') }}
  where (
      call_type_code = 'missed'
      or (call_type_code = 'incoming' and dialog_seq = 1)
      or is_real_transfer_hop
    )
  {% if is_incremental() %}
    and entry_started_at >= now() - toIntervalHour({{ lookback_hours }})
  {% endif %}
)

select
  entry_id,
  toUInt32(
    row_number() over (
      partition by entry_id
      order by started_at, dialog_seq
    )
  ) as event_seq,
  multiIf(
    call_type_code = 'missed', 'missed',
    call_type_code = 'incoming', 'incoming',
    is_real_transfer_hop, 'transfer',
    'other'
  ) as event_type,
  started_at,
  entry_started_at,
  user_id,
  user_name,
  group_id,
  group_name,
  queue_group_id,
  queue_group_name,
  client_phone,
  wait_to_accept_sec as wait_sec,
  talk_duration as talk_sec,
  transfer_kind,
  transfer_from_user_id,
  transfer_from_user_name,
  transfer_from_group_id,
  transfer_from_group_name,
  transfer_to_user_id,
  transfer_to_user_name,
  transfer_success,
  dialog_seq as source_dialog_seq
from raw_events
