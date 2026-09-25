{{
  config(
    materialized='view',
    alias='mart_operational_load_v',
    tags=['mart_fsd', 'helpdesk', 'operational_load'],
  )
}}

select *
from {{ ref('mart_helpdesk__operational_load_daily') }}
