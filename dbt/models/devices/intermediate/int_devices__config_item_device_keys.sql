{{
  config(
    materialized='table',
    alias='esp_ci_device_keys',
    engine=ch_engine_merge_tree(),
    order_by='(device_config_item_id)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

select
  ci.id as device_config_item_id,
  nullIf(trimBoth(ci.inventory_number), '') as rnm,
  nullIf(trimBoth(ci.serial_number), '') as znid,
  ci.sku_id as sku_id,
  ci.place_id as place_id,
  ci.parent_id as parent_id,
  ci.item_id as item_id
from {{ ref('int_helpdesk__config_items_current_v') }} as ci
where ci.deleted_at is null
  and trimBoth(coalesce(ci.inventory_number, '')) != ''
  and trimBoth(coalesce(ci.serial_number, '')) != ''
