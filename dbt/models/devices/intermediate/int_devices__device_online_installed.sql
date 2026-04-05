{{
  config(
    materialized='table',
    alias='esp_device_online_installed',
    engine=ch_engine_merge_tree(),
    order_by='(cashdesk_id)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

select
  cashdesk_id,
  first_online_mark_at as first_online_installed_at,
  true as is_installed
from (
  select *
  from {{ ref('int_devices__device_mark_events') }}
  where first_online_mark_at is not null
  order by cashdesk_id, _dbt_loaded_at desc
  limit 1 by cashdesk_id
)
