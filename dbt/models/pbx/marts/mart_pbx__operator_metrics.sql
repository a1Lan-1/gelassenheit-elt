{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p_day',
    alias='operator_metrics',
    engine=ch_engine_merge_tree(),
    order_by='(p_day, user_id)',
    partition_by='toYYYYMM(p_day)',
    settings={'allow_nullable_key': 1},
    tags=['mart_pbx', 'pbx', 'operator_metrics'],
  )
}}

-- Daily metrics by operator. delete+insert by p_day for lookback.
{% set lookback_days = var('pbx_marts_lookback_days', 3) | int %}

select
  toDate(assumeNotNull(started_at)) as p_day,
  assumeNotNull(user_id) as user_id,
  any(user_name) as user_name,
  any(user_extension) as user_extension,
  any(group_name) as group_name,
  count() as calls_answered,
  countIf(call_type_code = 'incoming') as incoming_answered,
  countIf(is_real_transfer_hop and call_type_code = 'transfered') as transfered_answered,
  countIf(is_real_transfer_hop and call_type_code = 'consult') as consult_answered,
  countIf(call_type_code = 'outgoing') as outgoing_answered,
  coalesce(avgIf(aht_talk_duration, aht_talk_duration is not null), 0) as avg_talk_sec,
  coalesce(avgIf(wait_to_accept_sec, call_type_code = 'incoming' or is_real_transfer_hop), 0) as avg_wait_sec,
  coalesce(avgIf(wait_to_accept_sec, is_real_transfer_hop), 0) as avg_transfer_wait_sec
from {{ ref('int_pbx__calls_detail') }}
where user_id is not null and user_id != ''
  and call_type_code != 'missed'
  {% if is_incremental() %}
  and started_at >= today() - {{ lookback_days }}
  {% endif %}
group by p_day, user_id
