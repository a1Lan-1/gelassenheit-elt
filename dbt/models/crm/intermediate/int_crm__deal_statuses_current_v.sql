{{
  config(
    materialized='view',
    alias='cur_statuses_v',
    tags=['cur_statuses', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__deal_statuses_current') }}
final