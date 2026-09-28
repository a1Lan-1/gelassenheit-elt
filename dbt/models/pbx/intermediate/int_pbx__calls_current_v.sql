{{
  config(
    materialized='view',
    alias='cur_calls_v',
    tags=['cur_calls', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__calls_current') }}
final