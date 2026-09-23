{{
  config(
    materialized='view',
    alias='cur_tasks_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__tasks_current') }}
final
