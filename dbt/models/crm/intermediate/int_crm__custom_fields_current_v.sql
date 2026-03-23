{{
  config(
    materialized='view',
    alias='cur_custom_fields_v',
    tags=['cur_custom_fields', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__custom_fields_current') }}
final
