{{
  config(
    materialized='view',
    alias='cur_sip_v',
    tags=['cur_sip', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__sip_current') }}
final
