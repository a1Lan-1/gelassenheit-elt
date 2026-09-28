{{
  config(
    materialized='view',
    alias='cur_deals_v',
    tags=['cur_deals', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__deals_current') }}
final