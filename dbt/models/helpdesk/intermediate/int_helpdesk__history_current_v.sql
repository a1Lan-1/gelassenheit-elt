{{
  config(
    materialized='view',
    alias='cur_history_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__history_current') }}
final