{{
  config(
    materialized='view',
    alias='cur_unsorted_v',
    tags=['cur_unsorted', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__unsorted_current') }}
final
