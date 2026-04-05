{{
  config(
    materialized='table',
    alias='esp_prepaid_license_positions',
    engine=ch_engine_merge_tree(),
    order_by='(license_key)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

with prepaid as (
  select
    pll.id as prepaid_list_id,
    pll.order_id as order_id,
    pll.subscription_id as subscription_id,
    pll.inn as client_inn,
    pll.kpp as client_kpp,
    pll.total_count as prepaid_total_count,
    pll.used_count as prepaid_used_count,
    greatest(pll.used_count - pll.total_count, 0) as prepaid_extra_count,
    pll.is_unlimit as is_unlimit,
    pll.date_created as prepaid_created_at,
    pll.date_auto_activated as date_auto_activated,
    pll.date_use_untill as date_use_untill,
    pll.date_last_used as date_last_used,
    pll.multi_inn_original_id as multi_inn_original_id,
    -- plan may be String/JSON; use JSONExtract (path/tupleElement on Nullable(JSON) fails on CH 26.2).
    ifNull(nullIf(JSONExtractString(toString(pll.plan), 'productCode'), ''), '') as tariff_product_code,
    toInt64OrNull(nullIf(JSONExtractString(toString(pll.plan), 'id'), '')) as plan_id,
    toInt64OrNull(nullIf(JSONExtractString(toString(pll.plan), 'price'), '')) as tariff_plan_amount,
    concat('prepaid-', toString(pll.subscription_id), '-', toString(pll.id)) as license_key,
    toUInt64(0) as cashdesk_id,
    pll.used_count as license_count,
    row_number() over (
      partition by pll.id
      order by pll.date_last_used desc nulls last, pll.date_created desc
    ) as row_num
  from mv_cashdesk.prepaid_license_list as pll final
  where pll.is_unlimit
    and pll.used_count > 0
),

orders_dim as (
  select
    o.id as order_id,
    o.date_payed as date_payed,
    o.status as status
  from mv_billing.orders as o final
  where o.id in (select order_id from prepaid where row_num = 1)
),

spu_dim as (
  select
    order_id,
    any(partner_code) as partner_code
  from mv_billing.subscription_payed_upd final
  where order_id in (select order_id from prepaid where row_num = 1)
  group by order_id
),

partners_dim as (
  select
    partner_code,
    inn
  from mv_portal.partners final
  where partner_code in (select partner_code from spu_dim)
),

company_by_inn as (
  select
    inn,
    anyHeavy(name) as name
  from mv_portal.company final
  where inn in (
    select client_inn from prepaid where row_num = 1
    union distinct
    select inn from partners_dim
  )
  group by inn
)

select
  p.license_key as license_key,
  p.cashdesk_id as cashdesk_id,
  p.order_id::UInt64 as order_id,
  p.subscription_id as subscription_id,
  '' as rnm,
  '' as znid,
  p.license_count as license_count,
  o.date_payed as paid_at,
  toDate(o.date_payed) as paid_date,
  coalesce(p.date_auto_activated, o.date_payed) as fin_date,
  toDate(coalesce(p.date_auto_activated, o.date_payed)) as activated_date,
  p.prepaid_created_at as active_from,
  p.date_use_untill as active_till,
  'PREPAID_UNLIMIT' as source_type,
  p.client_inn as client_inn,
  clkk.name as client_name,
  spu.partner_code as payment_partner_code,
  subclkk.name as payment_partner_name,
  '' as vendor,
  '' as model,
  toString(p.plan_id) as plan_id,
  p.tariff_product_code as tariff_product_code,
  toDecimal64(p.tariff_plan_amount, 4) as tariff_plan_amount,
  toDecimal64(0, 4) as license_amount,
  (o.date_payed is not null and o.status = 90) as is_paid,
  (
    p.date_auto_activated is not null
    and p.date_auto_activated < now()
  ) as is_activated,
  'prepaid_unlimited' as license_source,
  true as is_unlimited_prepaid,
  toInt64(p.prepaid_list_id) as prepaid_list_id,
  toInt32(p.prepaid_used_count) as prepaid_used_count,
  toInt32(p.prepaid_total_count) as prepaid_total_count,
  toInt32(p.prepaid_extra_count) as prepaid_extra_count,
  cast(p.multi_inn_original_id as Nullable(Int64)) as multi_inn_original_id
from prepaid as p
inner join orders_dim as o
  on o.order_id = p.order_id
left join company_by_inn as clkk
  on clkk.inn = p.client_inn
left join spu_dim as spu
  on spu.order_id = p.order_id
left join partners_dim as subp
  on subp.partner_code = spu.partner_code
left join company_by_inn as subclkk
  on subclkk.inn = subp.inn
where p.row_num = 1
  and p.client_inn not in ('112233445573')
