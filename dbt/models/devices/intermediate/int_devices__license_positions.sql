{{
  config(
    materialized='table',
    alias='esp_license_positions',
    engine=ch_engine_merge_tree(),
    order_by='(license_key)',
    tags=['esp', 'mart_esp', 'license_funnel'],
  )
}}

with prepaid_unlimited_orders as (
  select distinct order_id
  from mv_cashdesk.prepaid_license_list final
  where is_unlimit
    and used_count > 0
),

company_by_inn as (
  select
    inn,
    anyHeavy(name) as name
  from mv_portal.company final
  group by inn
),

spu_base as (
  select
    spu.subscription_id as subscription_id,
    spu.cashdesk_id as cashdesk_id,
    spu.order_id as order_id,
    spu.rnm as rnm,
    spu.znid as znid,
    spu.license_count as license_count,
    spu.fin_date as fin_date,
    spu.active_from as active_from,
    spu.active_till as active_till,
    spu.source_type as source_type,
    spu.inn as inn,
    spu.partner_code as partner_code,
    spu.vendor as vendor,
    spu.model as model,
    spu.plan_id as plan_id,
    spu.amount as amount
  from mv_billing.subscription_payed_upd as spu final
  where spu.license_count > 0
),

orders_dim as (
  select
    o.id as order_id,
    o.date_payed as date_payed,
    o.status as status
  from mv_billing.orders as o final
  where o.id in (select order_id from spu_base)
),

cashdesk_dim as (
  select
    c.id as id,
    c.rnm as rnm,
    c.znid as znid,
    c.inn as inn,
    c.vendor as vendor,
    c.model as model,
    c.cashdesk_type as cashdesk_type
  from mv_cashdesk.cashdesk as c final
  where c.id in (select cashdesk_id from spu_base where cashdesk_id > 0)
),

plan_dim as (
  select
    spl.id as id,
    spl.product_code as product_code,
    spl.amount as amount
  from mv_billing.subscription_plan as spl final
  where spl.id in (select plan_id from spu_base)
),

partners_dim as (
  select
    partner_code,
    inn
  from mv_portal.partners final
  where partner_code in (select partner_code from spu_base)
),

standard as (
  select
    concat(toString(spu.subscription_id), '-', toString(spu.cashdesk_id)) as license_key,
    spu.cashdesk_id::UInt64 as cashdesk_id,
    spu.order_id::UInt64 as order_id,
    spu.subscription_id as subscription_id,
    coalesce(nullIf(trimBoth(c.rnm), ''), nullIf(trimBoth(spu.rnm), '')) as rnm,
    coalesce(nullIf(trimBoth(c.znid), ''), nullIf(trimBoth(spu.znid), '')) as znid,
    toInt32(spu.license_count) as license_count,
    o.date_payed as paid_at,
    toDate(o.date_payed) as paid_date,
    spu.fin_date as fin_date,
    toDate(spu.fin_date) as activated_date,
    spu.active_from as active_from,
    spu.active_till as active_till,
    spu.source_type as source_type,
    coalesce(nullIf(trimBoth(c.inn), ''), nullIf(trimBoth(spu.inn), '')) as client_inn,
    clkk.name as client_name,
    spu.partner_code as payment_partner_code,
    subclkk.name as payment_partner_name,
    coalesce(nullIf(trimBoth(c.vendor), ''), nullIf(trimBoth(spu.vendor), '')) as vendor,
    coalesce(nullIf(trimBoth(c.model), ''), nullIf(trimBoth(spu.model), '')) as model,
    toString(spu.plan_id) as plan_id,
    spl.product_code as tariff_product_code,
    toDecimal64(spl.amount, 4) as tariff_plan_amount,
    toDecimal64(spu.amount, 4) as license_amount,
    (o.date_payed is not null and o.status = 90) as is_paid,
    (spu.fin_date is not null and spu.fin_date < now()) as is_activated,
    'subscription_payed_upd' as license_source,
    false as is_unlimited_prepaid,
    toInt64(0) as prepaid_list_id,
    toInt32(0) as prepaid_used_count,
    toInt32(0) as prepaid_total_count,
    toInt32(0) as prepaid_extra_count,
    cast(null as Nullable(Int64)) as multi_inn_original_id,
    row_number() over (
      partition by spu.subscription_id, spu.cashdesk_id
      order by spu.fin_date desc, o.date_payed desc, spu.active_from desc
    ) as row_num
  from spu_base as spu
  inner join orders_dim as o
    on o.order_id = spu.order_id
  left join cashdesk_dim as c
    on spu.cashdesk_id > 0
    and c.id = spu.cashdesk_id
  left join plan_dim as spl
    on spl.id = spu.plan_id
  left join company_by_inn as clkk
    on clkk.inn = coalesce(nullIf(trimBoth(c.inn), ''), nullIf(trimBoth(spu.inn), ''))
  left join partners_dim as subp
    on subp.partner_code = spu.partner_code
  left join company_by_inn as subclkk
    on subclkk.inn = subp.inn
  where coalesce(nullIf(trimBoth(c.inn), ''), nullIf(trimBoth(spu.inn), '')) not in ('112233445573')
    and (
      (
        spu.cashdesk_id = 0
        and spu.order_id not in prepaid_unlimited_orders
      )
      or (
        spu.cashdesk_id > 0
        and c.id > 0
        and c.cashdesk_type = 'NORMAL'
        and coalesce(nullIf(trimBoth(c.rnm), ''), nullIf(trimBoth(spu.rnm), '')) != ''
        and coalesce(nullIf(trimBoth(c.znid), ''), nullIf(trimBoth(spu.znid), '')) != ''
      )
    )
),

prepaid as (
  select
    license_key,
    cashdesk_id,
    order_id,
    subscription_id,
    rnm,
    znid,
    toInt32(license_count) as license_count,
    paid_at,
    paid_date,
    fin_date,
    activated_date,
    active_from,
    active_till,
    source_type,
    client_inn,
    client_name,
    payment_partner_code,
    payment_partner_name,
    vendor,
    model,
    toString(plan_id) as plan_id,
    tariff_product_code,
    tariff_plan_amount,
    license_amount,
    is_paid,
    is_activated,
    license_source,
    is_unlimited_prepaid,
    prepaid_list_id,
    prepaid_used_count,
    prepaid_total_count,
    prepaid_extra_count,
    multi_inn_original_id,
    1 as row_num
  from {{ ref('int_devices__prepaid_license_positions') }}
)

select
  license_key,
  cashdesk_id,
  order_id,
  subscription_id,
  rnm,
  znid,
  license_count,
  paid_at,
  paid_date,
  fin_date,
  activated_date,
  active_from,
  active_till,
  source_type,
  client_inn,
  client_name,
  payment_partner_code,
  payment_partner_name,
  vendor,
  model,
  plan_id,
  tariff_product_code,
  tariff_plan_amount,
  license_amount,
  is_paid,
  is_activated,
  license_source,
  is_unlimited_prepaid,
  prepaid_list_id,
  prepaid_used_count,
  prepaid_total_count,
  prepaid_extra_count,
  multi_inn_original_id
from standard
where row_num = 1

union all

select
  license_key,
  cashdesk_id,
  order_id,
  subscription_id,
  rnm,
  znid,
  license_count,
  paid_at,
  paid_date,
  fin_date,
  activated_date,
  active_from,
  active_till,
  source_type,
  client_inn,
  client_name,
  payment_partner_code,
  payment_partner_name,
  vendor,
  model,
  plan_id,
  tariff_product_code,
  tariff_plan_amount,
  license_amount,
  is_paid,
  is_activated,
  license_source,
  is_unlimited_prepaid,
  prepaid_list_id,
  prepaid_used_count,
  prepaid_total_count,
  prepaid_extra_count,
  multi_inn_original_id
from prepaid
