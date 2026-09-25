{{
  config(
    materialized='view',
    alias='mart_ticket_sla_v',
    tags=['mart_fsd', 'helpdesk', 'operational_load'],
  )
}}

select *
from {{ ref('mart_helpdesk__ticket_sla_daily') }}
