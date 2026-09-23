{{
  config(
    materialized='view',
    alias='cur_services_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__services_current') }}
final