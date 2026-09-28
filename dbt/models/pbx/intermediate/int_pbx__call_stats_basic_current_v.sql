{{
  config(
    materialized='view',
    alias='cur_call_stats_basic_v',
    tags=['cur_call_stats_basic', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__call_stats_basic_current') }}
final