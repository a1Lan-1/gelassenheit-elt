{{
  config(
    materialized='view',
    alias='cur_statuses_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select
  * except (`data`),
  CAST(toString(`data`), 'Nullable(String)') AS `data`
from {{ ref('int_helpdesk__statuses_current') }}
final