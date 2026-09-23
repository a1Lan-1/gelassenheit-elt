{{
  config(
    materialized='view',
    alias='cur_statuses_history_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__ticket_status_current') }}
final