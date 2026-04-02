{{
  config(
    materialized='view',
    alias='akc1_techsup_schedule_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__akc1_techsup_schedule_current') }}

