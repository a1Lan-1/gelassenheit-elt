{{
  config(
    materialized='view',
    alias='cur_sku_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__sku_current') }}
final