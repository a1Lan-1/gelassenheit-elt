{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date_transfer', 'transfer_initiator'],
    alias='v_transfer_by_operators_stats',
    engine=ch_engine_merge_tree(),
    order_by='(date_transfer, transfer_initiator)',
    settings={'allow_nullable_key': 1},
    tags=['mart_dialer', 'dialer', 'transfer'],
  )
}}

{% set lookback_days = var('dialer_marts_lookback_days', 3) | int %}

with phones_with_transfer as (
  select distinct client_phone
  from {{ ref('int_dialer__calls_detail') }}
  where call_type_code = 'transfered'
    and client_phone != ''
  {% if is_incremental() %}
    and started_at >= today() - {{ lookback_days }}
  {% endif %}
),

calls as (
  select
    started_at,
    call_type_code,
    scenario_result_name as result,
    transfer_initiator,
    missed_reason,
    duration,
    client_phone,
    row_number() over (partition by client_phone order by started_at) as rn
  from {{ ref('int_dialer__calls_detail') }}
  where client_phone in (select client_phone from phones_with_transfer)
  {% if is_incremental() %}
    and started_at >= today() - {{ lookback_days }}
  {% endif %}
),

results as (
  select
    curr.started_at,
    curr.call_type_code,
    curr.transfer_initiator,
    curr.missed_reason,
    curr.duration,
    curr.result as curr_result,
    prev.result as prev_result,
    multiIf(
      curr.missed_reason is null and coalesce(curr.duration, 0) > 0,
      1,
      0
    ) as is_success
  from calls as curr
  left join calls as prev
    on curr.client_phone = prev.client_phone
    and curr.rn = prev.rn + 1
  where curr.call_type_code = 'transfered'
)

select
  toDate(started_at) as date_transfer,
  transfer_initiator,
  count() as volume_transfered,
  countIf(is_success = 1) as volume_transfered_success,
  countIf(is_success = 0) as volume_transfered_fail,
  countIf(is_success = 0 and prev_result = 'Issue resolved') as volume_transfered_fail_after_success,
  countIf(curr_result in ('Issue resolved', 'Issue not resolved')) as volume_transfered_success_and_fail
from results
group by date_transfer, transfer_initiator
