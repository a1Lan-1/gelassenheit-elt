{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p_begin',
    alias='main_metrics',
    engine=ch_engine_merge_tree(),
    order_by='(p_begin)',
    partition_by='toYYYYMM(p_begin)',
    settings={'allow_nullable_key': 1},
    tags=['mart_pbx', 'pbx', 'main_metrics'],
  )
}}

-- Hourly CC KPIs Increment: lookback days by started_at.
{% set lookback_days = var('pbx_marts_lookback_days', 3) | int %}

with metrics as (
  select
    toStartOfHour(assumeNotNull(started_at)) as p_begin,
    countIf(call_type_code in ('incoming', 'missed') or is_real_transfer_hop) as count_call,
    countIf(call_type_code = 'incoming') as incoming_call,
    countIf(call_type_code = 'outgoing') as outgoing_call,
    countIf(call_type_code = 'missed') as missed_call,
    countIf(is_real_transfer_hop and call_type_code = 'transfered') as transfered_call,
    countIf(is_real_transfer_hop and call_type_code = 'consult') as consult_call,
    coalesce(
      avgIf(aht_talk_duration, call_type_code = 'incoming' or is_real_transfer_hop),
      0
    ) as aht_sec_bez_acw,
    coalesce(
      avgIf(wait_to_accept_sec, call_type_code = 'incoming' or is_real_transfer_hop),
      0
    ) as awt_sec,
    coalesce(
      avgIf(asa_sec, call_type_code = 'incoming' or is_real_transfer_hop),
      0
    ) as asa_sec,
    countIf(
      (call_type_code = 'incoming' or is_real_transfer_hop)
        and coalesce(wait_to_accept_sec, 999) <= 30
    ) as sl_count,
    coalesce(
      100.0 * countIf(
        (call_type_code = 'incoming' or is_real_transfer_hop)
          and coalesce(wait_to_accept_sec, 999) <= 30
      )
        / nullIf(countIf(call_type_code in ('incoming', 'missed') or is_real_transfer_hop), 0),
      0
    ) as sla,
    coalesce(
      100.0 * countIf(call_type_code = 'missed')
        / nullIf(countIf(call_type_code in ('incoming', 'missed') or is_real_transfer_hop), 0),
      0
    ) as lcr
  from {{ ref('int_pbx__calls_detail') }}
  {% if is_incremental() %}
  where started_at >= today() - {{ lookback_days }}
  {% endif %}
  group by p_begin
)

select
  p_begin,
  count_call,
  incoming_call,
  outgoing_call,
  missed_call,
  transfered_call,
  consult_call,
  awt_sec,
  asa_sec,
  aht_sec_bez_acw,
  sl_count,
  sla,
  lcr
from metrics
