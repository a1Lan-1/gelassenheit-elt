{{
  config(
    materialized='table',
    alias='mart_telephony_main_metrics',
    engine=ch_engine_merge_tree(),
    order_by='(p_begin, user_group_name)',
    partition_by='toYYYYMM(p_begin)',
    settings={'allow_nullable_key': 1},
    tags=['mart_telephony', 'telephony', 'main_metrics'],
  )
}}

{# Hourly telephony KPIs (logic as mart_pbx__main_metrics) by unified 1L/2L #}

select
  toStartOfHour(assumeNotNull(started_at)) as p_begin,
  assumeNotNull(user_group_name) as user_group_name,
  any(line_code) as line_code,
  countIf(call_type_code in ('incoming', 'missed') or is_real_transfer_hop = 1) as count_call,
  countIf(call_type_code = 'incoming') as incoming_call,
  countIf(call_type_code = 'outgoing') as outgoing_call,
  countIf(call_type_code = 'missed') as missed_call,
  countIf(is_real_transfer_hop = 1 and call_type_code = 'transfered') as transfered_call,
  countIf(is_real_transfer_hop = 1 and call_type_code = 'consult') as consult_call,
  coalesce(
    avgIf(aht_talk_duration, call_type_code = 'incoming' or is_real_transfer_hop = 1),
    0
  ) as aht_sec_bez_acw,
  coalesce(
    avgIf(wait_to_accept_sec, call_type_code = 'incoming' or is_real_transfer_hop = 1),
    0
  ) as awt_sec,
  coalesce(
    avgIf(asa_sec, call_type_code = 'incoming' or is_real_transfer_hop = 1),
    0
  ) as asa_sec,
  countIf(
    (call_type_code = 'incoming' or is_real_transfer_hop = 1)
      and coalesce(wait_to_accept_sec, 999) <= 30
  ) as sl_count,
  coalesce(
    100.0 * countIf(
      (call_type_code = 'incoming' or is_real_transfer_hop = 1)
        and coalesce(wait_to_accept_sec, 999) <= 30
    )
      / nullIf(countIf(call_type_code in ('incoming', 'missed') or is_real_transfer_hop = 1), 0),
    0
  ) as sla,
  coalesce(
    100.0 * countIf(call_type_code = 'missed')
      / nullIf(countIf(call_type_code in ('incoming', 'missed') or is_real_transfer_hop = 1), 0),
    0
  ) as lcr,
  countIf(source = 'pbx') as pbx_call,
  countIf(source = 'dialer') as dialer_call
from {{ ref('int_telephony__calls_l12') }}
where user_group_name is not null and user_group_name != ''
group by p_begin, user_group_name
