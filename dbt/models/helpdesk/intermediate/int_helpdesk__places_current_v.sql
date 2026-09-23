{{
  config(
    materialized='view',
    alias='cur_places_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__places_current') }}
final