{{
  config(
    materialized='view',
    alias='cur_roles_v',
    tags=['cur_roles', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__roles_current') }}
final
