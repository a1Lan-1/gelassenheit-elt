{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['period_date', 'period_type', 'detail_level', 'team_name', 'full_name'],
    alias='operators_daily_weekly_monthly',
    engine=ch_engine_merge_tree(),
    order_by='(period_date, period_type, detail_level, team_name, full_name)',
    settings={'allow_nullable_key': 1},
    tags=['mart_dialer', 'dialer', 'operators'],
  )
}}

{% set lookback_days = var('dialer_marts_lookback_days', 3) | int %}

with operator_groups as (
  select distinct
    user_id,
    email,
    full_name,
    multiIf(
      positionCaseInsensitive(email, '@onecta.ru') > 0, 'onecta',
      positionCaseInsensitive(email, '@example.com') > 0, 'esp',
      'other'
    ) as team_name
  from {{ ref('int_dialer__hours_current_v') }}
  where coalesce(user_id, 0) > 0
),
calls_data as (
  select
    toStartOfHour(assumeNotNull(started_at)) as date_hour,
    user_id,
    countIf(call_type_code = 'incoming') as incoming_calls,
    countIf(call_type_code = 'outgoing') as outgoing_calls,
    countIf(call_type_code in ('incoming', 'outgoing')) as total_handled_calls
  from {{ ref('int_dialer__calls_detail') }}
  where coalesce(user_id, 0) != 0
    and call_type_code in ('incoming', 'outgoing')
  {% if is_incremental() %}
    and started_at >= today() - {{ lookback_days }}
  {% endif %}
  group by date_hour, user_id
),
work_data as (
  select
    date_h,
    user_id,
    full_name,
    coalesce(status_normal, 0) as ready_sec,
    coalesce(status_ringing, 0) as ringing_sec,
    coalesce(status_speaking, 0) as speaking_sec,
    coalesce(status_wrapup, 0) as wrapup_sec,
    coalesce(status_normal, 0) + coalesce(status_ringing, 0) + coalesce(status_speaking, 0) + coalesce(status_wrapup, 0) as total_work_sec,
    coalesce(status_ringing, 0) + coalesce(status_speaking, 0) + coalesce(status_wrapup, 0) as handling_sec,
    toDate(date_h) as work_date,
    toStartOfWeek(date_h) as week_start,
    toStartOfMonth(date_h) as month_start
  from {{ ref('int_dialer__hours_current_v') }}
  where coalesce(user_id, 0) > 0
),
combined_data as (
  select
    coalesce(w.date_h, c.date_hour) as datetime_hour,
    coalesce(w.user_id, c.user_id) as src_user_id,
    w.full_name as src_full_name,
    og.team_name as src_team_name,
    coalesce(w.total_work_sec, 0) as total_work_sec,
    coalesce(w.handling_sec, 0) as handling_sec,
    coalesce(w.ready_sec, 0) as ready_sec,
    coalesce(w.ringing_sec, 0) as work_ringing_sec,
    coalesce(w.speaking_sec, 0) as work_speaking_sec,
    coalesce(w.wrapup_sec, 0) as work_wrapup_sec,
    coalesce(c.incoming_calls, 0) as incoming_calls,
    coalesce(c.outgoing_calls, 0) as outgoing_calls,
    coalesce(c.total_handled_calls, 0) as total_handled_calls,
    toDate(coalesce(w.date_h, c.date_hour)) as base_date,
    toStartOfWeek(coalesce(w.date_h, c.date_hour)) as base_week,
    toStartOfMonth(coalesce(w.date_h, c.date_hour)) as base_month
  from work_data as w
  full outer join calls_data as c
    on w.date_h = c.date_hour
    and w.user_id = c.user_id
  left join operator_groups as og
    on coalesce(w.user_id, c.user_id) = og.user_id
),
aggregated_data as (
  select
    multiIf(
      grouping(base_date) = 0 and grouping(base_week) = 1 and grouping(base_month) = 1, 'Day',
      grouping(base_date) = 1 and grouping(base_week) = 0 and grouping(base_month) = 1, 'Week',
      grouping(base_date) = 1 and grouping(base_week) = 1 and grouping(base_month) = 0, 'Month',
      null
    ) as period_type,
    multiIf(
      grouping(base_date) = 0 and grouping(base_week) = 1 and grouping(base_month) = 1, toString(base_date),
      grouping(base_date) = 1 and grouping(base_week) = 0 and grouping(base_month) = 1, concat('W', formatDateTime(base_week, '%G-%V')),
      grouping(base_date) = 1 and grouping(base_week) = 1 and grouping(base_month) = 0, formatDateTime(base_month, '%Y-%m'),
      null
    ) as period_value,
    multiIf(
      grouping(base_date) = 0 and grouping(base_week) = 1 and grouping(base_month) = 1, toDateTime(base_date),
      grouping(base_date) = 1 and grouping(base_week) = 0 and grouping(base_month) = 1, base_week,
      grouping(base_date) = 1 and grouping(base_week) = 1 and grouping(base_month) = 0, base_month,
      null
    ) as period_date,
    if(grouping(src_team_name) = 0 and grouping(src_user_id) = 1, src_team_name, null) as team_name,
    if(grouping(src_user_id) = 0, src_user_id, null) as user_id,
    if(grouping(src_user_id) = 0, max(src_full_name), null) as full_name,
    multiIf(
      grouping(src_user_id) = 0, 'operator',
      grouping(src_team_name) = 0, 'team',
      'total'
    ) as detail_level,
    sum(incoming_calls) as incoming_calls,
    sum(outgoing_calls) as outgoing_calls,
    sum(total_handled_calls) as total_handled_calls,
    sum(total_work_sec) as total_work_seconds,
    round(sum(total_work_sec) / 3600.0, 2) as total_work_hours,
    sum(ready_sec) as ready_seconds,
    sum(work_ringing_sec) as ringing_seconds,
    sum(work_speaking_sec) as speaking_seconds,
    sum(work_wrapup_sec) as wrapup_seconds,
    if(
      sum(total_work_sec) > 0,
      round(100.0 * sum(handling_sec) / sum(total_work_sec), 2),
      0
    ) as occupancy_percent
  from combined_data
  group by grouping sets (
    (base_date),
    (base_date, src_team_name),
    (base_date, src_team_name, src_user_id),
    (base_week),
    (base_week, src_team_name),
    (base_week, src_team_name, src_user_id),
    (base_month),
    (base_month, src_team_name),
    (base_month, src_team_name, src_user_id)
  )
)
select
  period_type,
  period_value,
  period_date,
  team_name,
  user_id,
  full_name,
  detail_level,
  incoming_calls,
  outgoing_calls,
  total_handled_calls,
  total_work_seconds,
  total_work_hours,
  ready_seconds,
  ringing_seconds,
  speaking_seconds,
  wrapup_seconds,
  occupancy_percent
from aggregated_data
where period_type is not null
