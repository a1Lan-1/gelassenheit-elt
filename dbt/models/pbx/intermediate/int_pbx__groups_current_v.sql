{{
  config(
    materialized='view',
    alias='cur_groups_v',
    tags=['cur_groups', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__groups_current') }}
final
