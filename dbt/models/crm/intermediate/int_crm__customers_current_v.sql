{{
  config(
    materialized='view',
    alias='cur_customers_v',
    tags=['cur_customers', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__customers_current') }}
final
