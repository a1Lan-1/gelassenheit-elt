{{
  config(
    materialized='view',
    alias='cur_ug_mapping_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__user_groups_mapping_current') }}
final