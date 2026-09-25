{{
  config(
    materialized='view',
    alias='mart_queue_sla_v',
    tags=['mart_fsd', 'helpdesk', 'operational_load'],
  )
}}

select *
from {{ ref('mart_helpdesk__queue_sla_daily') }}
