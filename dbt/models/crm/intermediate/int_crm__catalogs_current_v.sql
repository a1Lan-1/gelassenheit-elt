{{
  config(
    materialized='view',
    alias='cur_catalogs_v',
    tags=['cur_catalogs', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__catalogs_current') }}
final
