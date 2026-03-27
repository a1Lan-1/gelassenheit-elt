{{
  config(
    materialized='view',
    alias='cur_fsd_items_v',
    tags=['bot', 'current_view'],
  )
}}

select *
from {{ ref('int_bot__fsd_items_current') }}
final
