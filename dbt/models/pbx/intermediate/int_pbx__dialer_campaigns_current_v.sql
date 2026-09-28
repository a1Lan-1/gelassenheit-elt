{{
  config(
    materialized='view',
    alias='cur_dialer_campaigns_v',
    tags=['cur_dialer_campaigns', 'current_view'],
  )
}}

select *
from {{ ref('int_pbx__dialer_campaigns_current') }}
final