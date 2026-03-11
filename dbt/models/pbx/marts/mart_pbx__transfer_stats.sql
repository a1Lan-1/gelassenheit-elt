{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='date_transfer',
    alias='v_transfer_stats',
    engine=ch_engine_merge_tree(),
    order_by='(date_transfer)',
    partition_by='toYYYYMM(date_transfer)',
    settings={'allow_nullable_key': 1},
    tags=['mart_pbx', 'pbx', 'transfer'],
  )
}}

-- Daily transfer volumes: only is_real_transfer_hop; wait = wait_to_accept_sec.
{% set lookback_days = var('pbx_marts_lookback_days', 3) | int %}

with transfers as (
  select
    started_at,
    call_type_code,
    is_blind_transfer,
    is_consultation,
    transfer_kind,
    transfer_success,
    wait_to_accept_sec
  from {{ ref('int_pbx__calls_detail') }}
  where is_real_transfer_hop
  {% if is_incremental() %}
    and started_at >= today() - {{ lookback_days }}
  {% endif %}
),

agg as (
  select
    toDate(started_at) as date_transfer,
    count() as volume_transfered,
    countIf(transfer_kind = 'blind') as volume_transfered_blind,
    countIf(transfer_kind = 'consult') as volume_transfered_consult,
    countIf(transfer_success) as volume_transfered_success,
    countIf(not transfer_success) as volume_transfered_fail,
    coalesce(avg(wait_to_accept_sec), 0) as avg_gap_sec,
    coalesce(avgIf(wait_to_accept_sec, call_type_code = 'transfered'), 0) as avg_gap_transfered_sec,
    coalesce(avgIf(wait_to_accept_sec, call_type_code = 'consult'), 0) as avg_gap_consult_sec
  from transfers
  group by date_transfer
)

select
  date_transfer,
  volume_transfered,
  volume_transfered_blind,
  volume_transfered_consult,
  toUInt64(0) as volume_transfered_hold,
  toUInt64(0) as volume_blind_and_consult,
  volume_transfered_success,
  volume_transfered_fail,
  if(volume_transfered > 0, volume_transfered_blind / volume_transfered, 0) as blind_share,
  if(volume_transfered > 0, volume_transfered_consult / volume_transfered, 0) as consult_share,
  toFloat64(0) as hold_share,
  avg_gap_sec,
  avg_gap_transfered_sec,
  avg_gap_consult_sec
from agg
