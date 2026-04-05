{{
  config(
    materialized='table',
    alias='license_funnel',
    engine=ch_engine_merge_tree(),
    order_by='(paid_date, assumeNotNull(tariff_product_code), assumeNotNull(rnm), assumeNotNull(znid), license_key)',
    settings={'allow_nullable_key': 1},
    tags=['mart_esp', 'esp', 'license_funnel'],
  )
}}

with base as (
  select
    lp.license_key as license_key,
    lp.cashdesk_id as cashdesk_id,
    lp.order_id as order_id,
    lp.subscription_id as subscription_id,
    lp.rnm as rnm,
    lp.znid as znid,
    lp.license_count as license_count,
    lp.client_inn as client_inn,
    lp.client_name as client_name,
    lp.payment_partner_code as payment_partner_code,
    lp.payment_partner_name as payment_partner_name,
    lp.vendor as vendor,
    lp.model as model,
    lp.plan_id as plan_id,
    lp.tariff_product_code as tariff_product_code,
    lp.tariff_plan_amount as tariff_plan_amount,
    lp.license_amount as license_amount,
    lp.active_till as active_till,
    lp.source_type as source_type,
    lp.license_source as license_source,
    lp.is_unlimited_prepaid as is_unlimited_prepaid,
    lp.prepaid_list_id as prepaid_list_id,
    lp.prepaid_used_count as prepaid_used_count,
    lp.prepaid_total_count as prepaid_total_count,
    lp.prepaid_extra_count as prepaid_extra_count,
    lp.multi_inn_original_id as multi_inn_original_id,
    olt.subscription_object_count as order_subscription_object_count,
    olt.unlimited_extra_qty as order_unlimited_extra_qty,
    olt.order_license_qty as order_license_qty,
    olt.has_unlimited_prepaid as order_has_unlimited_prepaid,
    lp.paid_at as paid_at,
    lp.paid_date as paid_date,
    lp.fin_date as fin_date,
    lp.activated_date as activated_date,
    lp.active_from as active_from,
    lp.is_paid as is_paid,
    lp.is_activated as is_activated,
    (
      (lp.license_source = 'prepaid_unlimited' and lp.is_paid)
      or coalesce(inst.is_installed, false)
    ) as is_installed,
    if(
      lp.license_source = 'prepaid_unlimited',
      lp.fin_date,
      inst.first_online_installed_at
    ) as first_online_installed_at,
    coalesce(tsk.has_install_task, false) as has_install_task,
    coalesce(tsk.task_count, 0) as task_count,
    tsk.first_task_at as first_task_at,
    tsk.last_task_at as last_task_at,
    coalesce(tsk.has_closed_task, false) as has_closed_task,
    coalesce(tsk.has_open_task, false) as has_open_task,
    coalesce(tsk.is_esp_group, false) as is_esp_group
  from {{ ref('int_devices__license_funnel_positions') }} as lp
  left join {{ ref('int_devices__order_license_totals') }} as olt
    on olt.order_id = lp.order_id
  left join {{ ref('int_devices__device_online_installed') }} as inst
    on lp.cashdesk_id > 0
    and lp.license_source != 'order_unassigned'
    and lp.cashdesk_id::UInt64 = inst.cashdesk_id
  left join {{ ref('int_devices__fsd_install_tasks_by_device') }} as tsk
    on lp.cashdesk_id > 0
    and lp.license_source != 'order_unassigned'
    and lp.cashdesk_id::UInt64 = tsk.cashdesk_id
),

scored as (
  select
    *,
    is_activated
      and is_installed
      and has_install_task
      and has_open_task as is_anomaly_online_open_task
  from base
)

select
  license_key,
  cashdesk_id,
  order_id,
  subscription_id,
  rnm,
  znid,
  license_count,
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
  active_till,
  source_type,
  license_source,
  is_unlimited_prepaid,
  prepaid_list_id,
  prepaid_used_count,
  prepaid_total_count,
  prepaid_extra_count,
  multi_inn_original_id,
  order_subscription_object_count,
  order_unlimited_extra_qty,
  order_license_qty,
  order_has_unlimited_prepaid,
  paid_at,
  paid_date,
  fin_date,
  activated_date,
  active_from,
  is_paid,
  is_activated,
  is_installed,
  first_online_installed_at,
  has_install_task,
  task_count,
  first_task_at,
  last_task_at,
  has_closed_task,
  has_open_task,
  is_esp_group,
  is_anomaly_online_open_task,
  multiIf(
    is_anomaly_online_open_task, 'online_open_task',
    is_installed and has_install_task and has_closed_task, 'via_task',
    is_installed and not has_install_task, 'self',
    not is_installed and has_install_task, 'pending',
    'none'
  ) as install_path,
  multiIf(
    is_anomaly_online_open_task, 'installed_online_task_open',
    is_installed and has_install_task and has_closed_task, 'installed_via_task',
    is_installed and not has_install_task, 'installed_self',
    is_activated and has_install_task and not is_installed, 'install_in_progress',
    is_activated, 'activated',
    is_paid, 'paid',
    'unpaid'
  ) as funnel_stage
from scored
