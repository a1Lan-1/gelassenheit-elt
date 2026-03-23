{{
  config(
    materialized='view',
    alias='cur_contacts_v',
    tags=['cur_contacts', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__contacts_current') }}
final
