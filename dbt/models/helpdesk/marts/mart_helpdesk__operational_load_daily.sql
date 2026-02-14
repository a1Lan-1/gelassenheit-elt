{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='activity_date',
    alias='mart_operational_load_daily',
    engine=ch_engine_merge_tree(),
    order_by='(activity_date, load_category, user_group_name, service_theme, new_channel)',
    partition_by='toYYYYMM(activity_date)',
    settings={'allow_nullable_key': 1},
    tags=['mart_fsd', 'helpdesk', 'operational_load'],
  )
}}

{# We recalculate only the last N days (agreed with operational_load_lookback_days).
   resumed — service for pause, not included in the load. #}
{% set lookback_days = var('operational_load_lookback_days', 3) | int %}

select
  toDate(event_at, 'Europe/Moscow') as activity_date,
  load_category,
  user_group_name,
  service_theme,
  new_channel,
  count() as event_count,
  uniqExact(ticket_id) as ticket_count
from {{ ref('int_helpdesk__ticket_lifecycle_events') }}
where load_category in (
  'escalated_in',
  'escalated_out',
  'completed',
  'deferred',
  'reopened'
)
{% if is_incremental() %}
  and toDate(event_at, 'Europe/Moscow') >= (
    select coalesce(max(activity_date), toDate('1970-01-01')) - interval {{ lookback_days }} day
    from {{ this }}
  )
{% endif %}
group by
  activity_date,
  load_category,
  user_group_name,
  service_theme,
  new_channel
