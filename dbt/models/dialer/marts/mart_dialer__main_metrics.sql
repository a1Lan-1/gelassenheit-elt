{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p_begin',
    alias='main_metrics',
    engine=ch_engine_merge_tree(),
    order_by='(p_begin)',
    settings={'allow_nullable_key': 1},
    tags=['mart_dialer', 'dialer', 'main_metrics'],
  )
}}

{% set lookback_days = var('dialer_marts_lookback_days', 3) | int %}

with metrics as (
  select
    toStartOfHour(assumeNotNull(started_at)) as date_hour,
    countIf(call_type_code in ('incoming', 'missed')) as count_call,
    countIf(call_type_code = 'incoming') as incoming_call,
    countIf(call_type_code = 'outgoing') as outgoing_call,
    countIf(call_type_code = 'missed') as missed_call,
    countIf(call_type_code = 'transfered') as transfered_call,
    coalesce(avgIf(duration, call_type_code = 'incoming'), 0) as aht_sec_bez_acw,
    countIf(call_type_code = 'incoming' and coalesce(waiting_on_line_time, 999) <= 30) as sl_count,
    coalesce(
      100.0 * countIf(call_type_code = 'incoming' and coalesce(waiting_on_line_time, 999) <= 30)
        / nullIf(countIf(call_type_code = 'incoming'), 0),
      0
    ) as sla
  from {{ ref('int_dialer__calls_detail') }}
  {% if is_incremental() %}
  where started_at >= today() - {{ lookback_days }}
  {% endif %}
  group by date_hour
),
operators as (
  select
    date_h,
    sum(coalesce(status_normal, 0) + coalesce(status_ringing, 0) + coalesce(status_speaking, 0) + coalesce(status_wrapup, 0)) as worktime,
    sum(coalesce(status_ringing, 0) + coalesce(status_speaking, 0) + coalesce(status_wrapup, 0)) as handling
  from {{ ref('int_dialer__hours_current_v') }}
  group by date_h
)
select
  coalesce(m.date_hour, o.date_h) as p_begin,
  m.count_call,
  m.incoming_call,
  m.outgoing_call,
  m.missed_call,
  m.transfered_call,
  m.aht_sec_bez_acw,
  m.sl_count,
  m.sla,
  o.worktime,
  o.handling,
  if(coalesce(o.worktime, 0) > 0, round(coalesce(o.worktime, 0) / 3600.0, 2), 0) as hour_km,
  if(
    coalesce(o.worktime, 0) > 0,
    round(100.0 * coalesce(o.handling, 0) / nullIf(o.worktime, 0), 2),
    0
  ) as occ,
  if(
    coalesce(o.worktime, 0) > 0,
    round(100.0 * (coalesce(o.handling, 0) + 10) / nullIf(o.worktime, 0), 2),
    0
  ) as occ_10_sec_percent
from metrics as m
full outer join operators as o on m.date_hour = o.date_h
