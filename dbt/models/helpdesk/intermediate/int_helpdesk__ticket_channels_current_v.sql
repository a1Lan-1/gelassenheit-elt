{{
  config(
    materialized='view',
    alias='cur_channels_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__ticket_channels_current') }}
final