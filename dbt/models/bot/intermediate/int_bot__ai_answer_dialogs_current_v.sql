{{
  config(
    materialized='view',
    alias='cur_ai_answer_dialogs_v',
    tags=['bot', 'current_view'],
  )
}}

select *
from {{ ref('int_bot__ai_answer_dialogs_current') }}
final
