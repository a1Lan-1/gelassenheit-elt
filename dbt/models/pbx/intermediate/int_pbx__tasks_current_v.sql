{{
  config(
    materialized='view',
    alias='cur_tasks_v',
    tags=['cur_tasks', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__tasks_current') }}
final