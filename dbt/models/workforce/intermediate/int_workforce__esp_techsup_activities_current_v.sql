{{
  config(
    materialized='view',
    alias='esp_techsup_activities_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__esp_techsup_activities_current') }}

