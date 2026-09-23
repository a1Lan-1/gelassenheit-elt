{{
  config(
    materialized='view',
    alias='cur_ci_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__config_items_current') }}
final