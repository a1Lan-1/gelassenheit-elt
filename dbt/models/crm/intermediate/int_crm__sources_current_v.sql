{{
  config(
    materialized='view',
    alias='cur_sources_v',
    tags=['cur_sources', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__sources_current') }}
final
