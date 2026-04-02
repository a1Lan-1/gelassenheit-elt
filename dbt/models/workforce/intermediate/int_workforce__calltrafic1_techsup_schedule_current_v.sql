{{
  config(
    materialized='view',
    alias='calltrafic1_techsup_schedule_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__calltrafic1_techsup_schedule_current') }}

