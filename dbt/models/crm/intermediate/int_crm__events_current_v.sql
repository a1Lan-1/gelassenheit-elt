{{
  config(
    materialized='view',
    alias='cur_events_v',
    tags=['cur_events', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__events_current') }}
final