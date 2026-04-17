-- Fails on recent PBX rows that violate basic current-model invariants.
-- Keeps the scope tight so legacy edge cases do not permanently red the suite.
select
  entry_id,
  started_at,
  missed_flag,
  is_consultation,
  is_blind_transfer,
  waiting_time,
  waiting_on_line_time,
  employee_assigned_at,
  employee_accepted_at
from {{ ref('int_pbx__calls_current_v') }}
where started_at >= today() - 30
  and (
    missed_flag is null
    or is_consultation is null
    or is_blind_transfer is null
    or waiting_time < 0
    or waiting_on_line_time < 0
    or employee_assigned_at is null
    or employee_accepted_at < started_at
  )
