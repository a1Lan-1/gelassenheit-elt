{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='activity_date',
    alias='mart_ticket_sla_daily',
    engine=ch_engine_merge_tree(),
    order_by='(activity_date, cohort_mode, sla_metric, resolution_state, duration_bucket, user_group_name, current_status_name, service_theme, new_channel)',
    partition_by='toYYYYMM(activity_date)',
    settings={'allow_nullable_key': 1},
    tags=['mart_fsd', 'helpdesk', 'operational_load'],
  )
}}

{# Daily full-cycle SLA aggregates.
   duration_* = wall; duration_active_* = wall minus deferred pause.
   BI deferral-pause filter switches columns. #}
{% set lookback_days = var('operational_load_lookback_days', 3) | int %}

with
sla as (
  select
    ticket_id,
    created_at,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name,
    first_touch_at,
    first_resolve_at,
    full_resolve_at,
    first_touch_seconds,
    first_resolve_seconds,
    full_resolve_seconds,
    first_touch_active_seconds,
    first_resolve_active_seconds,
    full_resolve_active_seconds,
    paused_seconds_to_now,
    has_first_touch,
    has_first_resolve,
    has_full_resolve
  from {{ ref('int_helpdesk__ticket_sla_current') }}
  {% if is_incremental() %}
  where created_at >= today() - {{ lookback_days }}
     or first_touch_at >= today() - {{ lookback_days }}
     or first_resolve_at >= today() - {{ lookback_days }}
     or full_resolve_at >= today() - {{ lookback_days }}
  {% endif %}
),

metrics_long as (
  select
    ticket_id,
    created_at,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name,
    'first_touch' as sla_metric,
    first_touch_at as achieved_at,
    has_first_touch as is_achieved,
    if(has_first_touch, first_touch_seconds, dateDiff('second', created_at, now())) as duration_seconds,
    if(
      has_first_touch,
      first_touch_active_seconds,
      greatest(dateDiff('second', created_at, now()) - coalesce(paused_seconds_to_now, 0), 0)
    ) as duration_active_seconds
  from sla

  union all

  select
    ticket_id,
    created_at,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name,
    'first_resolve' as sla_metric,
    first_resolve_at as achieved_at,
    has_first_resolve as is_achieved,
    if(has_first_resolve, first_resolve_seconds, dateDiff('second', created_at, now())) as duration_seconds,
    if(
      has_first_resolve,
      first_resolve_active_seconds,
      greatest(dateDiff('second', created_at, now()) - coalesce(paused_seconds_to_now, 0), 0)
    ) as duration_active_seconds
  from sla

  union all

  select
    ticket_id,
    created_at,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name,
    'full_resolve' as sla_metric,
    full_resolve_at as achieved_at,
    has_full_resolve as is_achieved,
    if(has_full_resolve, full_resolve_seconds, dateDiff('second', created_at, now())) as duration_seconds,
    if(
      has_full_resolve,
      full_resolve_active_seconds,
      greatest(dateDiff('second', created_at, now()) - coalesce(paused_seconds_to_now, 0), 0)
    ) as duration_active_seconds
  from sla
),

metrics_b as (
  select
    *,
    multiIf(
      duration_seconds < 3600,
      '<1h',
      duration_seconds < 14400,
      '1-4h',
      duration_seconds < 86400,
      '4-24h',
      duration_seconds < 259200,
      '1-3d',
      duration_seconds < 604800,
      '3-7d',
      '>7d'
    ) as duration_bucket,
    if(is_achieved, 'achieved', 'pending') as resolution_state
  from metrics_long
),

created_cohort as (
  select
    toDate(created_at, 'Europe/Moscow') as activity_date,
    'created' as cohort_mode,
    sla_metric,
    resolution_state,
    duration_bucket,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name,
    count() as ticket_count,
    avg(duration_seconds) as avg_duration_seconds,
    quantileExact(0.5)(duration_seconds) as p50_duration_seconds,
    quantileExact(0.8)(duration_seconds) as p80_duration_seconds,
    quantileExact(0.9)(duration_seconds) as p90_duration_seconds,
    quantileExact(0.99)(duration_seconds) as p99_duration_seconds,
    avg(duration_active_seconds) as avg_duration_active_seconds,
    quantileExact(0.5)(duration_active_seconds) as p50_duration_active_seconds,
    quantileExact(0.8)(duration_active_seconds) as p80_duration_active_seconds,
    quantileExact(0.9)(duration_active_seconds) as p90_duration_active_seconds,
    quantileExact(0.99)(duration_active_seconds) as p99_duration_active_seconds
  from metrics_b
  {% if is_incremental() %}
  where toDate(created_at, 'Europe/Moscow') >= (
    select coalesce(max(activity_date), toDate('1970-01-01')) - interval {{ lookback_days }} day
    from {{ this }}
  )
  {% endif %}
  group by
    activity_date,
    sla_metric,
    resolution_state,
    duration_bucket,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name
),

achieved_cohort as (
  select
    toDate(achieved_at, 'Europe/Moscow') as activity_date,
    'achieved' as cohort_mode,
    sla_metric,
    'achieved' as resolution_state,
    duration_bucket,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name,
    count() as ticket_count,
    avg(duration_seconds) as avg_duration_seconds,
    quantileExact(0.5)(duration_seconds) as p50_duration_seconds,
    quantileExact(0.8)(duration_seconds) as p80_duration_seconds,
    quantileExact(0.9)(duration_seconds) as p90_duration_seconds,
    quantileExact(0.99)(duration_seconds) as p99_duration_seconds,
    avg(duration_active_seconds) as avg_duration_active_seconds,
    quantileExact(0.5)(duration_active_seconds) as p50_duration_active_seconds,
    quantileExact(0.8)(duration_active_seconds) as p80_duration_active_seconds,
    quantileExact(0.9)(duration_active_seconds) as p90_duration_active_seconds,
    quantileExact(0.99)(duration_active_seconds) as p99_duration_active_seconds
  from metrics_b
  where is_achieved
    and achieved_at is not null
    {% if is_incremental() %}
    and toDate(achieved_at, 'Europe/Moscow') >= (
      select coalesce(max(activity_date), toDate('1970-01-01')) - interval {{ lookback_days }} day
      from {{ this }}
    )
    {% endif %}
  group by
    activity_date,
    sla_metric,
    resolution_state,
    duration_bucket,
    service_theme,
    new_channel,
    user_group_name,
    current_status_name
)

select * from created_cohort
union all
select * from achieved_cohort
