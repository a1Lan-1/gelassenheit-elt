{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='activity_date',
    alias='mart_queue_sla_daily',
    engine=ch_engine_merge_tree(),
    order_by='(activity_date, cohort_mode, user_group_name, exit_reason, duration_bucket, service_theme, new_channel)',
    partition_by='toYYYYMM(activity_date)',
    settings={'allow_nullable_key': 1},
    tags=['mart_fsd', 'helpdesk', 'operational_load'],
  )
}}

{# Daily queue SLA by episode; cohort_mode=entry|exit.
   duration_* = wall; duration_active_* = wall minus deferred pause.
   duration_bucket on wall. Lookback: operational_load_lookback_days. #}
{% set lookback_days = var('operational_load_lookback_days', 3) | int %}

with
episodes as (
  select
    ticket_id,
    user_group_name,
    entry_at,
    exit_at,
    exit_reason,
    duration_seconds,
    duration_active_seconds,
    service_theme,
    new_channel
  from {{ ref('int_helpdesk__ticket_queue_episodes') }}
),

episodes_b as (
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
    ) as duration_bucket
  from episodes
),

entry_cohort as (
  select
    toDate(entry_at, 'Europe/Moscow') as activity_date,
    'entry' as cohort_mode,
    user_group_name,
    exit_reason,
    duration_bucket,
    service_theme,
    new_channel,
    count() as episode_count,
    uniqExact(ticket_id) as ticket_count,
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
  from episodes_b
  {% if is_incremental() %}
  where toDate(entry_at, 'Europe/Moscow') >= (
    select coalesce(max(activity_date), toDate('1970-01-01')) - interval {{ lookback_days }} day
    from {{ this }}
  )
  {% endif %}
  group by
    activity_date,
    user_group_name,
    exit_reason,
    duration_bucket,
    service_theme,
    new_channel
),

exit_cohort as (
  select
    toDate(exit_at, 'Europe/Moscow') as activity_date,
    'exit' as cohort_mode,
    user_group_name,
    exit_reason,
    duration_bucket,
    service_theme,
    new_channel,
    count() as episode_count,
    uniqExact(ticket_id) as ticket_count,
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
  from episodes_b
  where exit_reason in ('completed', 'escalated_out')
    and exit_at is not null
    {% if is_incremental() %}
    and toDate(exit_at, 'Europe/Moscow') >= (
      select coalesce(max(activity_date), toDate('1970-01-01')) - interval {{ lookback_days }} day
      from {{ this }}
    )
    {% endif %}
  group by
    activity_date,
    user_group_name,
    exit_reason,
    duration_bucket,
    service_theme,
    new_channel
)

select * from entry_cohort
union all
select * from exit_cohort
