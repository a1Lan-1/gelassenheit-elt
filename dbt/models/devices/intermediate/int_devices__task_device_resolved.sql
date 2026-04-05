{{
  config(
    materialized='table',
    alias='esp_task_device_resolved',
    engine=ch_engine_merge_tree(),
    order_by='(task_id, cashdesk_id)',
    settings={'allow_nullable_key': 1},
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

{# Extract JSON path on config_items without table alias (ci.data.key fails on CH 26.2). #}
with config_items as (
  select
    id,
    item_id,
    sku_id,
    updated_at,
    deleted_at,
    assumeNotNull(
      toUInt64OrZero(ifNull(JSONExtractString(toString(data), 'cashdeskId'), ''))
    ) as cashdesk_id
  from {{ ref('int_helpdesk__config_items_current_v') }}
  where deleted_at is null
),

task_lines as (
  select
    t.id as task_id,
    t.number as task_number,
    t.ticket_id as ticket_id,
    t.created_at as task_created_at,
    t.updated_at as task_updated_at,
    st.name as status_name,
    ug.name as user_group_name,
    tci.config_item_id as link_item_id,
    ci.id as config_item_id,
    ci.cashdesk_id as cashdesk_id,
    sku.type as sku_type,
    sku.name as sku_name,
    sku.vendor_code as sku_vendor_code,
    row_number() over (
      partition by t.id, ci.cashdesk_id
      order by ci.updated_at desc, ci.id
    ) as row_num
  from {{ ref('int_helpdesk__tasks_current_v') }} as t
  inner join {{ ref('int_helpdesk__task_config_item_current_v') }} as tci
    on tci.task_id = t.id
    and tci.deleted_at is null
  inner join config_items as ci
    on ci.item_id = tci.config_item_id
  left join {{ ref('int_helpdesk__sku_current_v') }} as sku
    on sku.id = ci.sku_id
  left join {{ ref('int_helpdesk__statuses_current_v') }} as st
    on st.id = t.status_id
  left join {{ ref('int_helpdesk__user_groups_current_v') }} as ug
    on ug.id = t.user_group_id
  where t.deleted_at is null
    and ci.cashdesk_id > 0
)

select
  task_id,
  task_number,
  ticket_id,
  task_created_at,
  task_updated_at,
  status_name,
  user_group_name,
  config_item_id,
  link_item_id,
  cashdesk_id,
  sku_type,
  sku_name,
  sku_vendor_code,
  'cashdeskId' as device_resolve_path,
  status_name = 'Closed' as is_closed,
  status_name is null or status_name != 'Closed' as is_open,
  lower(coalesce(user_group_name, '')) like '%support%' as is_esp_group
from task_lines
where row_num = 1
