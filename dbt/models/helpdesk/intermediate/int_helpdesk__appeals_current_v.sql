{{
  config(
    materialized='view',
    alias='cur_appeals_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__appeals_current') }}
final