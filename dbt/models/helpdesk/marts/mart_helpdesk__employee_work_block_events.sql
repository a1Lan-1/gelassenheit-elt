{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_date',
    alias='mart_employee_work_block_events',
    engine=ch_engine_merge_tree(),
    order_by='(event_at, user_id)',
    partition_by='toYYYYMM(work_date)',
    settings={'allow_nullable_key': 1},
    tags=['mart_fsd', 'helpdesk', 'work_blocks'],
  )
}}

{% set lookback_days = var('work_block_lookback_days', 14) | int %}

with events as (
  select
    b.user_id as user_id,
    u.user_name as user_name,
    b.work_date as work_date,
    b.block_no as block_no,
    'work' as status,
    b.block_start as event_at
  from {{ ref('int_helpdesk__employee_work_blocks') }} as b
  left join {{ ref('stg_helpdesk__employee_work_users') }} as u
    on u.user_id = b.user_id
  {% if is_incremental() %}
  where b.work_date >= today() - {{ lookback_days }}
  {% endif %}

  union all

  select
    b.user_id,
    u.user_name,
    b.work_date,
    b.block_no,
    'work interrupted' as status,
    b.block_end as event_at
  from {{ ref('int_helpdesk__employee_work_blocks') }} as b
  left join {{ ref('stg_helpdesk__employee_work_users') }} as u
    on u.user_id = b.user_id
  {% if is_incremental() %}
  where b.work_date >= today() - {{ lookback_days }}
  {% endif %}

  union all

  select
    c.user_id,
    u.user_name,
    c.work_date,
    cast(null as Nullable(UInt32)) as block_no,
    'call' as status,
    c.call_start as event_at
  from {{ ref('stg_helpdesk__employee_call_intervals') }} as c
  left join {{ ref('stg_helpdesk__employee_work_users') }} as u
    on u.user_id = c.user_id
  {% if is_incremental() %}
  where c.work_date >= today() - {{ lookback_days }}
  {% endif %}
)

select
  user_name,
  status,
  event_at,
  work_date,
  user_id,
  block_no,
  now() as _dbt_loaded_at
from events
where trimBoth(user_name) != ''
