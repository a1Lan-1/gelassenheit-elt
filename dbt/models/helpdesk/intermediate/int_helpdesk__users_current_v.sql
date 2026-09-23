{{
  config(
    materialized='view',
    alias='cur_users_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__users_current') }}
final