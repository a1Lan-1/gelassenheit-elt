{{
  config(
    materialized='view',
    alias='cur_ai_usage_daily_v',
    tags=['bot', 'current_view'],
  )
}}

select *
from {{ ref('int_bot__ai_usage_daily_current') }}
final
