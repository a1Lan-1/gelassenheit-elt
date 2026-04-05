{{
  config(
    materialized='table',
    alias='esp_install_helpdesk_by_cashdesk',
    engine=ch_engine_merge_tree(),
    order_by='(cashdesk_id)',
    settings={'allow_nullable_key': 1},
    tags=['mart_esp', 'esp', 'install_report'],
  )
}}

{# Helpdesk spine for install_report: one row per cashdesk_id. #}
select
  assumeNotNull(toUInt64(ht.cashdesk_id)) as cashdesk_id,
  argMax(hr.created, ht.updated) as ticket_created_at,
  argMax(hr.sd_number, ht.updated) as ticket_number,
  argMax(ht.created, ht.updated) as task_created_at,
  argMax(ht.sd_number, ht.updated) as task_number,
  argMax(ht.status, ht.updated) as task_status_code,
  argMax(ht.partner_code, ht.updated) as task_partner_code,
  argMax(
    concat(su.last_name, ' ', su.first_name, ' ', su.middle_name),
    ht.updated
  ) as installer_name,
  max(ht.updated) as last_task_status_at,
  argMax(ht.completion_confirmed_date, ht.updated) as esm_confirmation_received_at,
  argMax(ht.completion_confirmed_date, ht.updated) is not null as esm_confirmation_received,
  argMax(st.name, t.updated_at) as fsd_status
from mv_helpdesk.helpdesk_tasks as ht final
inner join mv_helpdesk.helpdesk_requests as hr final on hr.id = ht.helpdesk_request_id
left any join mv_helpdesk.sd_user as su final on ht.sd_responsible_user_id = su.sd_user_id
any left join {{ ref('int_helpdesk__tasks_current_v') }} t on t.number = ht.sd_number
any left join {{ ref('int_helpdesk__statuses_current_v') }} st on st.id = t.status_id
where ht.cashdesk_id is not null
  and ht.cashdesk_id > 0
group by ht.cashdesk_id
