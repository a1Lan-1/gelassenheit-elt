{{
  config(
    materialized='table',
    alias='esp_order_license_totals',
    engine=ch_engine_merge_tree(),
    order_by='(order_id)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

with orders_typed as (
  select
    o.id::UInt64 as order_id,
    o.status as order_status,
    o.date_created as date_created,
    o.date_payed as date_payed
  from mv_billing.orders as o final
  where o.status = 90
),

subscription_content as (
  select
    oc.order_id::UInt64 as order_id,
    sum(oc.object_count) as subscription_object_count
  from mv_billing.order_content as oc final
  where oc.content_type = 'SUBSCRIPTION'
  group by oc.order_id
),

unlimited_extra as (
  select
    pll.order_id::UInt64 as order_id,
    sum(pll.used_count - pll.total_count) as unlimited_extra_qty,
    max(pll.is_unlimit) as has_unlimited_prepaid
  from mv_cashdesk.prepaid_license_list as pll final
  where pll.is_unlimit
    and pll.used_count > 0
  group by pll.order_id
)

select
  o.order_id as order_id,
  o.order_status as order_status,
  o.date_created as date_created,
  toDate(o.date_created) as order_created_date,
  o.date_payed as date_payed,
  toDate(o.date_payed) as paid_date,
  coalesce(sc.subscription_object_count, 0) as subscription_object_count,
  coalesce(ue.unlimited_extra_qty, 0) as unlimited_extra_qty,
  coalesce(sc.subscription_object_count, 0) + coalesce(ue.unlimited_extra_qty, 0) as order_license_qty,
  coalesce(ue.has_unlimited_prepaid, false) as has_unlimited_prepaid
from orders_typed as o
inner join subscription_content as sc on sc.order_id = o.order_id
left join unlimited_extra as ue on ue.order_id = o.order_id
