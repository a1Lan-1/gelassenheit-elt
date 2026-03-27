{{
  config(
    materialized='view',
    alias='cur_ai_bot_sessions_v',
    tags=['bot', 'current_view'],
  )
}}

select *
from {{ ref('int_bot__ai_bot_sessions_current') }}
final
