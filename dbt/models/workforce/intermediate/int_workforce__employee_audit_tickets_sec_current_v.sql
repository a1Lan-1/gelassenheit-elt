{{
  config(
    materialized='view',
    alias='employee_audit_tickets_sec_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__employee_audit_tickets_sec_current') }}

