{{
  config(
    materialized='view',
    alias='cur_companies_v',
    tags=['cur_companies', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__companies_current') }}
final