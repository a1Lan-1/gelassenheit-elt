{{
  config(
    materialized='view',
    alias='cur_categories_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__ticket_categories_current') }}
final