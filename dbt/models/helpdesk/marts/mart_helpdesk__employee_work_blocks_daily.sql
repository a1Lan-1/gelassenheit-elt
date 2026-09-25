{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_date',
    alias='mart_employee_work_blocks_daily',
    engine=ch_engine_merge_tree(),
    order_by='(work_date, user_id)',
    partition_by='toYYYYMM(work_date)',
    settings={'allow_nullable_key': 1},
    tags=['mart_fsd', 'helpdesk', 'work_blocks'],
  )
}}

{% set lookback_days = var('work_block_lookback_days', 14) | int %}

with work_daily as (
  select
    user_id,
    work_date,
    count() as block_count,
    sum(block_sec) as work_sec
  from {{ ref('int_helpdesk__employee_work_blocks') }}
  {% if is_incremental() %}
  where work_date >= today() - {{ lookback_days }}
  {% endif %}
  group by user_id, work_date
),

daily_keys as (
  select user_id, work_date from work_daily
  union distinct
  select user_id, work_date
  from {{ ref('stg_helpdesk__employee_work_schedule') }}
  {% if is_incremental() %}
  where work_date >= today() - {{ lookback_days }}
  {% endif %}
)

select
  k.user_id as user_id,
  u.user_name as user_name,
  u.email as email,
  k.work_date as work_date,
  s.scheduled_hours as scheduled_hours,
  coalesce(w.block_count, 0) as block_count,
  coalesce(w.work_sec, 0) as work_sec,
  round(coalesce(w.work_sec, 0) / 3600.0, 2) as work_hours,
  round(coalesce(w.work_sec, 0) / 3600.0 - coalesce(s.scheduled_hours, 0), 2) as delta_hours,
  if(
    coalesce(s.scheduled_hours, 0) > 0,
    round((coalesce(w.work_sec, 0) / 3600.0) / s.scheduled_hours, 3),
    null
  ) as work_to_schedule_ratio,
  now() as _dbt_loaded_at
from daily_keys as k
left join work_daily as w
  on w.user_id = k.user_id
  and w.work_date = k.work_date
left join {{ ref('stg_helpdesk__employee_work_users') }} as u
  on u.user_id = k.user_id
left join {{ ref('stg_helpdesk__employee_work_schedule') }} as s
  on s.user_id = k.user_id
  and s.work_date = k.work_date
where trimBoth(u.user_name) != ''
