{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p_day',
    alias='transfer_edges',
    engine=ch_engine_merge_tree(),
    order_by='(p_day, from_user_id, to_user_id)',
    partition_by='toYYYYMM(p_day)',
    settings={'allow_nullable_key': 1},
    tags=['mart_pbx', 'pbx', 'transfer_edges'],
  )
}}

-- Translation edges who→whom: only is_real_transfer_hop. delete+insert by p_day.
{% set lookback_days = var('pbx_marts_lookback_days', 3) | int %}

select
  toDate(assumeNotNull(started_at)) as p_day,
  assumeNotNull(transfer_from_user_id) as from_user_id,
  assumeNotNull(transfer_to_user_id) as to_user_id,
  count() as transfers,
  countIf(is_blind_transfer or transfer_kind = 'blind') as blind_transfers,
  countIf(call_type_code = 'consult' or transfer_kind = 'consult') as consult_transfers,
  toUInt64(0) as hold_transfers,
  countIf(transfer_success) as successful_transfers,
  coalesce(avg(wait_to_accept_sec), 0) as avg_gap_sec
from {{ ref('int_pbx__calls_detail') }}
where is_real_transfer_hop
  and transfer_from_user_id is not null
  and transfer_to_user_id is not null
  and transfer_from_user_id != ''
  and transfer_to_user_id != ''
  {% if is_incremental() %}
  and started_at >= today() - {{ lookback_days }}
  {% endif %}
group by p_day, from_user_id, to_user_id
