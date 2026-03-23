{{
  config(
    materialized='view',
    alias='cur_catalog_elements_v',
    tags=['cur_catalog_elements', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__catalog_elements_current') }}
final
