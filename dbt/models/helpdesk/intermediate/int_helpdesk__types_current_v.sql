{{
  config(
    materialized='view',
    alias='cur_types_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__types_current') }}
final