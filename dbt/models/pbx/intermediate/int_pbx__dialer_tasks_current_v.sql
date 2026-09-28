{{
  config(
    materialized='view',
    alias='cur_dialer_tasks_v',
    tags=['cur_dialer_tasks', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__dialer_tasks_current') }}
final