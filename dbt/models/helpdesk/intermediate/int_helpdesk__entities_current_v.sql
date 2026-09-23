{{
  config(
    materialized='view',
    alias='cur_entities_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__entities_current') }}
final