{{
  config(
    materialized='view',
    alias='cur_hours_v',
    tags=['cur_hours', 'current_view'],
  )
}}

select *
from {{ ref('int_dialer__hours_current') }}
final