{{
  config(
    materialized='view',
    alias='cur_tasks_v',
    tags=['cur_tasks', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__tasks_current') }}
final
