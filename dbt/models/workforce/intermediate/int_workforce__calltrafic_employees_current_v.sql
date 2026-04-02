{{
  config(
    materialized='view',
    alias='calltrafic_employees_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__calltrafic_employees_current') }}

