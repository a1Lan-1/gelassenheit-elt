{{
  config(
    materialized='view',
    alias='cur_cases_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__ticket_cases_current') }}
final