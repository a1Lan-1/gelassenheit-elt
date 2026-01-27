{{
  config(
    materialized='view',
    alias='stg_employee_work_schedule',
    tags=['helpdesk', 'work_blocks', 'staging'],
  )
}}

-- Planned hours from all workforce sheets.
with schedule_raw as (
  select
    toDate(s.date) as work_date,
    trimBoth(s.employee) as employee_name,
    toFloat64(s.hours) as scheduled_hours
  from {{ ref('int_workforce__esp_techsup_schedule_current_v') }} as s
  where s.date is not null
    and trimBoth(s.employee) != ''

  union all

  select
    toDate(s.date) as work_date,
    trimBoth(s.employee) as employee_name,
    toFloat64(s.hours) as scheduled_hours
  from {{ ref('int_workforce__akc1_techsup_schedule_current_v') }} as s
  where s.date is not null
    and trimBoth(s.employee) != ''

  union all

  select
    toDate(s.date) as work_date,
    trimBoth(s.user_name) as employee_name,
    toFloat64(s.work_hours) as scheduled_hours
  from {{ ref('int_workforce__akc2_techsup_schedule_current_v') }} as s
  where s.date is not null
    and trimBoth(s.user_name) != ''
),

schedule_daily as (
  select
    work_date,
    employee_name,
    sum(coalesce(scheduled_hours, 0)) as scheduled_hours
  from schedule_raw
  group by work_date, employee_name
)

select
  u.user_id as user_id,
  s.work_date as work_date,
  s.scheduled_hours as scheduled_hours
from schedule_daily as s
inner join {{ ref('stg_helpdesk__employee_work_users') }} as u
  on trimBoth(s.employee_name) = trimBoth(u.user_name)
where s.employee_name != ''
