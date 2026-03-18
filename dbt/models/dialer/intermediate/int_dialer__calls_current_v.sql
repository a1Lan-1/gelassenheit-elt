{{
  config(
    materialized='view',
    alias='cur_calls_v',
    tags=['cur_calls', 'current_view'],
  )
}}

-- Exclude broken telemetry: negative waiting_on_line_time must not enter analytics.
select *
from {{ ref('int_dialer__calls_current') }}
final
where waiting_on_line_time is null
   or waiting_on_line_time >= 0
