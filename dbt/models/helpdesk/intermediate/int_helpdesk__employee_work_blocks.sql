{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_date',
    alias='int_employee_work_blocks',
    engine=ch_engine_merge_tree(),
    order_by='(user_id, work_date, block_start)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'work_blocks', 'current'],
  )
}}

{% set idle_seconds = var('work_block_idle_seconds', 300) %}
{% set lookback_days = var('work_block_lookback_days', 14) | int %}
-- call_end in stg includes +2 min after outbound (dialer + PBX)

with
  actions_raw as (
    select user_id, event_at as ts, work_date
    from {{ ref('stg_helpdesk__employee_work_history_events') }}
    {% if is_incremental() %}
    where work_date >= today() - {{ lookback_days }}
    {% endif %}
  ),

  actions_tagged as (
    select
      user_id,
      ts,
      work_date,
      dateDiff(
        'second',
        lagInFrame(ts) over (partition by user_id, work_date order by ts),
        ts
      ) as gap_sec
    from actions_raw
  ),

  actions_sessions as (
    select
      user_id,
      work_date,
      ts,
      sum(if(gap_sec > {{ idle_seconds }} or gap_sec is null, 1, 0))
        over (partition by user_id, work_date order by ts) as sess_id
    from actions_tagged
  ),

  action_intervals as (
    select
      user_id,
      work_date,
      min(ts) as block_start,
      max(ts) + toIntervalSecond({{ idle_seconds }}) as block_end
    from actions_sessions
    group by user_id, work_date, sess_id
  ),

  call_intervals as (
    select
      user_id,
      work_date,
      call_start as block_start,
      call_end as block_end
    from {{ ref('stg_helpdesk__employee_call_intervals') }}
    {% if is_incremental() %}
    where work_date >= today() - {{ lookback_days }}
    {% endif %}
  ),

  all_intervals as (
    select user_id, work_date, block_start, block_end from action_intervals
    union all
    select user_id, work_date, block_start, block_end from call_intervals
  ),

  intervals_sorted as (
    select
      user_id,
      work_date,
      block_start,
      block_end,
      max(block_end) over (
        partition by user_id, work_date
        order by block_start, block_end
        rows between unbounded preceding and 1 preceding
      ) as prev_end
    from all_intervals
  ),

  intervals_marked as (
    select
      *,
      sum(
        if(
          prev_end is null
          or block_start > prev_end + toIntervalSecond({{ idle_seconds }}),
          1,
          0
        )
      ) over (partition by user_id, work_date order by block_start, block_end) as grp
    from intervals_sorted
  ),

  blocks_merged as (
    select
      user_id,
      work_date,
      grp as block_no,
      min(block_start) as block_start,
      max(block_end) as block_end
    from intervals_marked
    group by user_id, work_date, grp
  )

select
  user_id,
  work_date,
  block_no,
  block_start,
  block_end,
  dateDiff('second', block_start, block_end) as block_sec,
  now() as _dbt_loaded_at
from blocks_merged
