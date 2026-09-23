{{
  config(
    materialized='view',
    alias='cur_tickets_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__tickets_current') }}
final