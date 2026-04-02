{{
  config(
    materialized='view',
    alias='akc_employees_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__akc_employees_current') }}

