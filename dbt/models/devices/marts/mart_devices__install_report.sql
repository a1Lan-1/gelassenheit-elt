{{
  config(
    materialized='table',
    alias='install_report',
    engine=ch_engine_merge_tree(),
    order_by='(cashdesk_id, assumeNotNull(subscription_id))',
    settings={'allow_nullable_key': 1},
    tags=['mart_esp', 'esp', 'install_report'],
  )
}}

{# Dedup on cur_install_report (Replacing): one current row per license.
   Multiple subscription_id per cashdesk_id are kept. #}
select
  cashdesk_id,
  subscription_id,
  client_inn,
  client_name,
  payment_partner_code,
  payment_partner_name,
  vendor,
  model,
  znid,
  rnm,
  fnid,
  address,
  license_id,
  license_paid_date,
  license_active_from,
  financial_date,
  license_created_date,
  tariff_product_code,
  tariff_amount,
  esm_installed,
  first_mark_check_date,
  first_mark_check_month_year,
  first_online_mark_check_date,
  first_online_mark_check_month_year,
  esm_install_ticket_created_date,
  esm_install_ticket_number,
  esm_install_task_created_date,
  esm_install_task_number,
  esm_install_task_status,
  fsd_status,
  esm_install_task_partner_code,
  esm_install_task_partner_name,
  installer_name,
  last_task_status_date,
  esm_confirmation_received_date,
  esm_confirmation_received,
  esm_version,
  order_partner_manager_name,
  task_partner_manager_name
from (
  select *
  from {{ ref('int_devices__install_report_current') }}
  order by cashdesk_id, subscription_id, _dbt_loaded_at desc
  limit 1 by cashdesk_id, subscription_id
)
