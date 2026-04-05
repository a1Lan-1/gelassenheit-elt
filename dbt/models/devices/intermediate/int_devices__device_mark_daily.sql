{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='esp_device_mark_daily',
    engine=ch_engine_merge_tree(),
    order_by='(event_day, cashdesk_id, type)',
    tags=['esp', 'mart_esp', 'license_funnel', 'install_report', 'device_mark'],
  )
}}

{#
  Daily grain of Local/Online marks from anl.aggregated_events.
  Watermark on event_day (PK prefix) for partition prune; overlap 1 day for late arrivals.
  device_mark_events aggregates this table (not raw events) for one row per cashdesk.
#}
select
  event_day,
  cashdesk_id,
  type,
  min(min_event_date_in_day) as min_event_at
from {{ source('anl', 'aggregated_events') }}
prewhere
  type in ('imcGismtDataLocal', 'imcGismtDataOnline')
  and has_error = false
  {% if is_incremental() %}
  and event_day >= (
    select coalesce(max(event_day), toDate('1970-01-01')) - 1
    from {{ this }}
  )
  {% endif %}
where cashdesk_id > 0
group by event_day, cashdesk_id, type
