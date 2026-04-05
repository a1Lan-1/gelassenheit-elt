{{
  config(
    materialized='table',
    alias='esp_order_unassigned_licenses',
    engine=ch_engine_merge_tree(),
    order_by='(order_id)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

with position_by_order as (
  select
    toUInt64(lp.order_id) as order_id,
    sum(lp.license_count) as position_license_qty,
    anyHeavy(lp.client_inn) as client_inn,
    anyHeavy(lp.client_name) as client_name,
    anyHeavy(lp.payment_partner_code) as payment_partner_code,
    anyHeavy(lp.payment_partner_name) as payment_partner_name,
    anyHeavy(lp.tariff_product_code) as tariff_product_code,
    anyHeavy(lp.plan_id) as plan_id,
    anyHeavy(lp.tariff_plan_amount) as tariff_plan_amount
  from {{ ref('int_devices__license_positions') }} as lp
  group by lp.order_id
),

gaps as (
  select
    o.order_id as order_id,
    o.date_payed as paid_at,
    o.paid_date as paid_date,
    o.order_license_qty - coalesce(p.position_license_qty, 0) as license_count,
    p.client_inn as client_inn,
    p.client_name as client_name,
    p.payment_partner_code as payment_partner_code,
    p.payment_partner_name as payment_partner_name,
    p.tariff_product_code as tariff_product_code,
    p.plan_id as plan_id,
    p.tariff_plan_amount as tariff_plan_amount
  from {{ ref('int_devices__order_license_totals') }} as o
  left join position_by_order as p
    on p.order_id = o.order_id
  where o.order_license_qty > coalesce(p.position_license_qty, 0)
)

select
  concat('order-', toString(order_id), '-unassigned') as license_key,
  toUInt64(0) as cashdesk_id,
  order_id::UInt64 as order_id,
  toInt64(0) as subscription_id,
  '' as rnm,
  '' as znid,
  toInt32(license_count) as license_count,
  paid_at,
  paid_date,
  cast(null as Nullable(DateTime64(3))) as fin_date,
  cast(null as Nullable(Date)) as activated_date,
  cast(null as Nullable(DateTime64(3))) as active_from,
  cast(null as Nullable(DateTime64(3))) as active_till,
  'ORDER_UNASSIGNED' as source_type,
  client_inn,
  client_name,
  payment_partner_code,
  payment_partner_name,
  '' as vendor,
  '' as model,
  plan_id,
  tariff_product_code,
  tariff_plan_amount,
  toDecimal64(0, 4) as license_amount,
  true as is_paid,
  false as is_activated,
  'order_unassigned' as license_source,
  false as is_unlimited_prepaid,
  toInt64(0) as prepaid_list_id,
  toInt32(0) as prepaid_used_count,
  toInt32(0) as prepaid_total_count,
  toInt32(0) as prepaid_extra_count,
  cast(null as Nullable(Int64)) as multi_inn_original_id
from gaps
