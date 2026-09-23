{{
  config(
    materialized='view',
    alias='cur_ticket_ci_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__ticket_config_item_current') }}
final