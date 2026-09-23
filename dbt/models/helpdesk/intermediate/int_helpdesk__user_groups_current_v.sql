{{
  config(
    materialized='view',
    alias='cur_user_groups_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__user_groups_current') }}
final