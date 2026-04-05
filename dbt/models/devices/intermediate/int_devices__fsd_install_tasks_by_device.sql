{{
  config(
    materialized='table',
    alias='esp_fsd_install_tasks',
    engine=ch_engine_merge_tree(),
    order_by='(assumeNotNull(cashdesk_id))',
    settings={'allow_nullable_key': 1},
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

select
  assumeNotNull(cashdesk_id)::UInt64 as cashdesk_id,
  true as has_install_task,
  count() as task_count,
  min(task_created_at) as first_task_at,
  max(task_updated_at) as last_task_at,
  countIf(is_closed) > 0 as has_closed_task,
  countIf(is_open) > 0 as has_open_task,
  countIf(is_esp_group) > 0 as is_esp_group,
  countIf(is_open) > 0 as has_open_task_with_online
from {{ ref('int_devices__task_device_resolved') }}
group by
  cashdesk_id
