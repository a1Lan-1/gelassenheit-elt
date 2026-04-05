{{
  config(
    materialized='table',
    alias='esp_license_funnel_positions',
    engine=ch_engine_merge_tree(),
    order_by='(license_key)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

select * from {{ ref('int_devices__license_positions') }}

union all

select * from {{ ref('int_devices__order_unassigned_licenses') }}
