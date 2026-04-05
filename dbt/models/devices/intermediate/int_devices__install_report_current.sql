{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['cashdesk_id', 'subscription_id'],
    alias='cur_install_report',
    engine=ch_engine_replacing('_dbt_loaded_at'),
    order_by='(cashdesk_id, assumeNotNull(subscription_id))',
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['mart_esp', 'esp', 'install_report', 'current'],
  )
}}

{# Raw install report: Replacing-append.
   Grain: cashdesk_id + subscription_id (multiple licenses per device on renewal is OK).
   Mart install_report reads limit 1 by / FINAL. #}
{% set cutoff = "toDateTime('2025-01-01')" %}
{% set lookback_days = var('install_report_lookback_days', 1) | int %}

with
{% if is_incremental() %}
touched_cashdesks as (
  -- Billing / new licenses
  select toUInt64(cashdesk_id) as cashdesk_id
  from mv_cashdesk.cashdesk_billing_info
  where cashdesk_id > 0
    and date_created >= today() - {{ lookback_days }}
  union distinct
  -- First mark / online-mark
  select cashdesk_id
  from (
    select *
    from {{ ref('int_devices__device_mark_events') }}
    order by cashdesk_id, _dbt_loaded_at desc
    limit 1 by cashdesk_id
  )
  where greatest(
      coalesce(first_mark_at, toDateTime('1970-01-01')),
      coalesce(first_online_mark_at, toDateTime('1970-01-01'))
    ) >= today() - {{ lookback_days }}
  union distinct
  -- Helpdesk status changes (keeps report fsd_status fresh)
  select toUInt64(cashdesk_id) as cashdesk_id
  from mv_helpdesk.helpdesk_tasks
  where cashdesk_id is not null
    and cashdesk_id > 0
    and updated >= today() - {{ lookback_days }}
),
{% endif %}

cashdesk_base as (
  select
    toUInt64(c.id) as cashdesk_id,
    c.inn as client_inn,
    c.vendor as vendor,
    c.model as model,
    c.znid as znid,
    c.rnm as rnm,
    c.fnid as fnid,
    c.address as address,
    c.esm_uid as esm_uid,
    c.esm_uid is not null as esm_installed
  from mv_cashdesk.cashdesk as c
  where c.id > 0
    and c.inn not in ('112233445573')
  {% if is_incremental() %}
    and toUInt64(c.id) in (select cashdesk_id from touched_cashdesks)
  {% endif %}
),

billing_base as (
  select
    toUInt64(cbi.cashdesk_id) as cashdesk_id,
    cbi.subscription_id as subscription_id,
    cbi.order_id as order_id,
    cbi.date_created as license_created_at,
    cbi.active_from as license_active_from
  from (
    select *
    from mv_cashdesk.cashdesk_billing_info as cbi
    where cbi.cashdesk_id > 0
      and cbi.cashdesk_id in (select toInt64(cashdesk_id) from cashdesk_base)
    order by cashdesk_id, subscription_id, date_created desc, active_from desc
    limit 1 by cashdesk_id, subscription_id
  ) as cbi
),

order_base as (
  select
    id as order_id,
    date_payed as license_paid_at
  from (
    select *
    from mv_billing.orders
    where id in (select order_id from billing_base)
    order by id, date_payed desc
    limit 1 by id
  )
),

plan_base as (
  select
    id as subscription_id,
    partner_code as payment_partner_code,
    plan_id as plan_id
  from (
    select *
    from mv_billing.subscription_payed
    where id in (select subscription_id from billing_base)
    order by id
    limit 1 by id
  )
),

tariff_base as (
  select
    id as plan_id,
    product_code as tariff_product_code,
    amount as tariff_amount
  from (
    select *
    from mv_billing.subscription_plan
    where id in (select plan_id from plan_base)
    order by id
    limit 1 by id
  )
),

company_dim as (
  select
    inn,
    anyHeavy(name) as name
  from (
    select *
    from mv_portal.company
    where inn in (
      select client_inn from cashdesk_base
      union distinct
      select p.inn
      from (
        select *
        from mv_portal.partners
        where partner_code in (select payment_partner_code from plan_base)
        order by partner_code
        limit 1 by partner_code
      ) as p
    )
    order by inn
    limit 1 by inn
  )
  group by inn
),

partners_dim as (
  select
    partner_code,
    inn,
    manager_id
  from (
    select *
    from mv_portal.partners
    where partner_code in (select payment_partner_code from plan_base)
    order by partner_code
    limit 1 by partner_code
  )
),

cd as (
  select
    cb.cashdesk_id as cashdesk_id,
    clkk.name as client_name,
    pb.payment_partner_code as payment_partner_code,
    subclkk.name as payment_partner_name,
    concat(toString(bb.subscription_id), '-', toString(cb.cashdesk_id)) as license_id,
    cb.client_inn as client_inn,
    cb.vendor as vendor,
    cb.model as model,
    cb.znid as znid,
    cb.rnm as rnm,
    cb.fnid as fnid,
    cb.address as address,
    eur.req_version as esm_version,
    bb.subscription_id as subscription_id,
    ob.license_paid_at as license_paid_at,
    bb.license_created_at as license_created_at,
    bb.license_active_from as license_active_from,
    tb.tariff_product_code as tariff_product_code,
    tb.tariff_amount as tariff_amount,
    cb.esm_installed as esm_installed,
    concat(man.surname, ' ', man.name, ' ', man.middlename) as order_partner_manager_name
  from cashdesk_base as cb
  inner join billing_base as bb on bb.cashdesk_id = cb.cashdesk_id
  inner join order_base as ob on ob.order_id = bb.order_id
  left any join (
    select *
    from mv_cashdesk.esm_update_request
    order by esm_uid
    limit 1 by esm_uid
  ) as eur on eur.esm_uid = cb.esm_uid
  any left join plan_base as pb on pb.subscription_id = bb.subscription_id
  any left join tariff_base as tb on tb.plan_id = pb.plan_id
  any left join company_dim as clkk on clkk.inn = cb.client_inn
  any left join partners_dim as subp on subp.partner_code = pb.payment_partner_code
  any left join company_dim as subclkk on subclkk.inn = subp.inn
  left any join (
    select *
    from mv_portal.user_profile
    order by user_id
    limit 1 by user_id
  ) as man on subp.manager_id = man.user_id
),

mark as (
  select
    cashdesk_id,
    first_mark_at,
    first_online_mark_at
  from (
    select *
    from {{ ref('int_devices__device_mark_events') }}
    where cashdesk_id in (select cashdesk_id from cashdesk_base)
    order by cashdesk_id, _dbt_loaded_at desc
    limit 1 by cashdesk_id
  )
),

hd as (
  select *
  from {{ ref('int_devices__install_helpdesk_by_cashdesk') }}
  where cashdesk_id in (select cashdesk_id from cashdesk_base)
),

task_partners as (
  select
    partner_code,
    inn,
    manager_id
  from (
    select *
    from mv_portal.partners
    where partner_code in (select task_partner_code from hd where task_partner_code != '')
    order by partner_code
    limit 1 by partner_code
  )
)

select
  cd.cashdesk_id as cashdesk_id,
  cd.subscription_id as subscription_id,
  cd.client_inn as client_inn,
  cd.client_name as client_name,
  cd.payment_partner_code as payment_partner_code,
  cd.payment_partner_name as payment_partner_name,
  cd.vendor as vendor,
  cd.model as model,
  cd.znid as znid,
  cd.rnm as rnm,
  cd.fnid as fnid,
  cd.address as address,
  cd.license_id as license_id,
  toDate(cd.license_paid_at) as license_paid_date,
  toDate(cd.license_active_from) as license_active_from,
  toDate(cd.license_active_from) as financial_date,
  toDate(cd.license_created_at) as license_created_date,
  cd.tariff_product_code as tariff_product_code,
  cd.tariff_amount as tariff_amount,
  cd.esm_installed as esm_installed,
  if(mark.first_mark_at < {{ cutoff }}, null, toDate(mark.first_mark_at)) as first_mark_check_date,
  if(
    mark.first_mark_at < {{ cutoff }},
    null,
    formatDateTime(mark.first_mark_at, '%c, %Y')
  ) as first_mark_check_month_year,
  if(mark.first_online_mark_at < {{ cutoff }}, null, toDate(mark.first_online_mark_at)) as first_online_mark_check_date,
  if(
    mark.first_online_mark_at < {{ cutoff }},
    null,
    formatDateTime(mark.first_online_mark_at, '%c, %Y')
  ) as first_online_mark_check_month_year,
  if(hd.ticket_created_at < {{ cutoff }}, null, toDate(hd.ticket_created_at)) as esm_install_ticket_created_date,
  hd.ticket_number as esm_install_ticket_number,
  if(hd.task_created_at < {{ cutoff }}, null, toDate(hd.task_created_at)) as esm_install_task_created_date,
  hd.task_number as esm_install_task_number,
  multiIf(
    hd.task_status_code = 0, '',
    hd.task_status_code = 40, 'new',
    hd.task_status_code = 50, 'in_progress',
    hd.task_status_code = 51, 'postponed',
    hd.task_status_code = 52, 'engineer_assigned',
    hd.task_status_code = 89, 'completed',
    hd.task_status_code = 90, 'closed',
    hd.task_status_code >= 100, 'error',
    'unknown'
  ) as esm_install_task_status,
  hd.fsd_status as fsd_status,
  hd.task_partner_code as esm_install_task_partner_code,
  if(hd.task_partner_code = '', '', task_partner_company.name) as esm_install_task_partner_name,
  hd.installer_name as installer_name,
  if(hd.last_task_status_at < {{ cutoff }}, null, toDate(hd.last_task_status_at)) as last_task_status_date,
  if(
    hd.esm_confirmation_received_at < {{ cutoff }},
    null,
    toDate(hd.esm_confirmation_received_at)
  ) as esm_confirmation_received_date,
  hd.esm_confirmation_received as esm_confirmation_received,
  cd.esm_version as esm_version,
  cd.order_partner_manager_name as order_partner_manager_name,
  concat(task_man.surname, ' ', task_man.name, ' ', task_man.middlename) as task_partner_manager_name,
  now64(3) as _dbt_loaded_at
from cd
any left join mark on cd.cashdesk_id = mark.cashdesk_id
any left join hd on cd.cashdesk_id = hd.cashdesk_id
any left join task_partners as taskp on taskp.partner_code = hd.task_partner_code
any left join company_dim as task_partner_company on task_partner_company.inn = taskp.inn
left any join (
  select *
  from mv_portal.user_profile
  order by user_id
  limit 1 by user_id
) as task_man on taskp.manager_id = task_man.user_id
