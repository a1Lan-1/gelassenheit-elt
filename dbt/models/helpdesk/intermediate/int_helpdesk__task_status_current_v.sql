{{
  config(
    materialized='view',
    alias='cur_task_status_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__task_status_current') }}
final