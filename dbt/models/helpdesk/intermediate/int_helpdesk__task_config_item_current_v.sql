{{
  config(
    materialized='view',
    alias='cur_task_ci_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__task_config_item_current') }}
final