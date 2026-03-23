{{
  config(
    materialized='view',
    alias='cur_users_v',
    tags=['cur_users', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__users_current') }}
final