{{
  config(
    materialized='view',
    alias='cur_tags_v',
    tags=['cur_tags', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__tags_current') }}
final
