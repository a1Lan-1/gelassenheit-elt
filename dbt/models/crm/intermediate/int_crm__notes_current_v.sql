{{
  config(
    materialized='view',
    alias='cur_notes_v',
    tags=['cur_notes', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__notes_current') }}
final
