{{
  config(
    materialized='view',
    alias='cur_user_types_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__user_types_current') }}
final