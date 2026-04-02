{{
  config(
    materialized='view',
    alias='itm_msb_targets_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__itm_msb_targets_current') }}

