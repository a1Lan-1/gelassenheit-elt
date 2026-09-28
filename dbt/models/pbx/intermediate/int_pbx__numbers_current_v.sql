{{
  config(
    materialized='view',
    alias='cur_numbers_v',
    tags=['cur_numbers', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__numbers_current') }}
final
