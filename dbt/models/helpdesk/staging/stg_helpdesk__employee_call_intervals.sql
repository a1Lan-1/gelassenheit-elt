{{
  config(
    materialized='table',
    alias='stg_employee_call_intervals',
    engine=ch_engine_merge_tree(),
    order_by='(work_date, user_id, call_start)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'work_blocks', 'staging'],
  )
}}

{% set call_extend_seconds = var('work_block_call_extend_seconds', 120) %}

with users_by_key as (
  select user_id, user_name_key as name_key
  from {{ ref('stg_helpdesk__employee_work_users') }}
  where user_name_key != ''
  union all
  select user_id, login_name_key as name_key
  from {{ ref('stg_helpdesk__employee_work_users') }}
  where login_name_key != ''
  union all
  select user_id, lowerUTF8(trimBoth(email)) as name_key
  from {{ ref('stg_helpdesk__employee_work_users') }}
  where nullIf(trimBoth(email), '') is not null
),

users_dedup as (
  select name_key, any(user_id) as user_id
  from users_by_key
  where name_key != ''
  group by name_key
),

dialer as (
  select
    u.user_id as user_id,
    c.started_at as call_start,
    c.started_at + toIntervalSecond(
      greatest(toInt64(coalesce(c.duration, 0)), 0)
      + if(ifNull(c.call_type_code, '') = 'outgoing', {{ call_extend_seconds }}, 0)
    ) as call_end,
    toDate(c.started_at) as work_date,
    'dialer' as call_source
  from {{ ref('int_dialer__calls_current_v') }} as c
  inner join users_dedup as u
    on lowerUTF8(trimBoth(c.user_name)) = u.name_key
  where trimBoth(c.user_name) != ''
    and (
      ifNull(c.call_type_code, '') = 'outgoing'
      or coalesce(c.duration, 0) > 0
    )
),

pbx_src as (
  select
    c.started_at as call_start,
    c.started_at + toIntervalSecond(
      greatest(
        toInt64(coalesce(c.talk_duration, 0)),
        toInt64(coalesce(c.duration, 0)),
        0
      )
      + if(ifNull(c.call_type_code, '') = 'outgoing', {{ call_extend_seconds }}, 0)
    ) as call_end,
    toDate(c.started_at) as work_date,
    lowerUTF8(trimBoth(ifNull(c.user_name, ''))) as user_name_key,
    lowerUTF8(trimBoth(ifNull(c.user_email, ''))) as user_email_key
  from {{ ref('int_pbx__calls_detail') }} as c
  where (
      ifNull(c.call_type_code, '') = 'outgoing'
      or coalesce(c.talk_duration, 0) > 0
    )
),

pbx as (
  select
    u.user_id as user_id,
    s.call_start as call_start,
    s.call_end as call_end,
    s.work_date as work_date,
    'pbx' as call_source
  from pbx_src as s
  inner join users_dedup as u
    on u.name_key = s.user_name_key
  where s.user_name_key != ''

  union all

  select
    u.user_id,
    s.call_start,
    s.call_end,
    s.work_date,
    'pbx' as call_source
  from pbx_src as s
  inner join users_dedup as u
    on u.name_key = s.user_email_key
  where s.user_email_key != ''
    and s.user_email_key != s.user_name_key
)

select
  user_id,
  call_start,
  max(call_end) as call_end,
  work_date,
  call_source
from (
  select * from dialer
  union all
  select * from pbx
)
group by user_id, call_start, work_date, call_source
