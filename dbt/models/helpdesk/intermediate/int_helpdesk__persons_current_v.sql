{{
  config(
    materialized='view',
    alias='cur_persons_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__persons_current') }}
final