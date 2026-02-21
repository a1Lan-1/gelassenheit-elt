{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='mart_ticket_actions',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['mart_fsd', 'helpdesk', 'ticket_actions', 'ticket_actions_core'],
  )
}}

with base as (
  select *
  from {{ ref('int_helpdesk__ticket_actions_base') }}
  {% if not is_incremental() %}
    final
  {% endif %}
  {% if is_incremental() %}
    where updated_at > (
      select coalesce(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
      from {{ this }}
    )
  {% endif %}
),

tickets as (
  select id, number
  from {{ ref('int_helpdesk__tickets_current') }} final
  where id in (select ticket_id from base where ticket_id is not null)
),

tasks as (
  select id, number
  from {{ ref('int_helpdesk__tasks_current') }} final
  where id in (select task_id from base where task_id is not null)
),

users as (
  select id, last_name, first_name, middle_name, email
  from {{ ref('int_helpdesk__users_current') }} final
),

statuses as (
  select id, name
  from {{ ref('int_helpdesk__statuses_current') }} final
),

user_groups as (
  select id, name
  from {{ ref('int_helpdesk__user_groups_current') }} final
)

select
  he.id as id,
  toString(tk.number) as ticket_number,
  he.ticket_id as ticket_id,
  toString(tsk_number.number) as task_number,
  trim(concat(u.last_name, ' ', u.first_name, ' ', u.middle_name)) as user_name,
  u.email as email,
  he.object_type as object_type,
  he.object_id as object_id,
  he.updated_at as updated_at,
  he.timer_value as timer_value,
  multiIf(
    he.object_type = 'appeals',
    concat(
      he.appeal_type, ' - ', he.appeal_source, ' - ', he.appeal_text, ' - ',
      multiIf(
        he.foreign_type = 'tickets', concat('Ticket: ', he.foreign_id),
        he.foreign_type = 'tasks', concat('Task: ', he.foreign_id),
        ''
      )
    ),
    he.appeal_info
  ) as appeal_info,
  he.call_duration as call_duration,
  he.appeal_text as appeal_text,
  he.comment_text as comment_text,
  he.notify_user as notify_user,
  {{ fsd_change_json_typed('he.old_description', 'he.new_description', 'old_description', 'new_description') }} as description_change,
  {{ fsd_change_json_typed('he.old_theme', 'he.new_theme', 'old_theme', 'new_theme') }} as theme_change,
  if(
    he.old_status_id is not null or he.new_status_id is not null,
    accurateCastOrNull(
      concat(
        '{"old_status":', toJSONString(coalesce(st_old.name, 'null')),
        ',"new_status":', toJSONString(coalesce(st_new.name, 'null')), '}'
      ),
      'JSON'
    ),
    CAST(NULL, 'Nullable(JSON)')
  ) as status_change,
  if(
    he.old_stage_id is not null or he.new_stage_id is not null,
    accurateCastOrNull(
      concat(
        '{"old_stage":', toJSONString(coalesce(st3.name, 'null')),
        ',"new_stage":', toJSONString(coalesce(st4.name, 'null')), '}'
      ),
      'JSON'
    ),
    CAST(NULL, 'Nullable(JSON)')
  ) as stage_change,
  if(
    he.old_responsible_id is not null or he.new_responsible_id is not null,
    accurateCastOrNull(
      concat(
        '{"old_responsible":', toJSONString(coalesce(nullIf(trim(concat(u2.last_name, ' ', u2.first_name, ' ', u2.middle_name)), ''), 'null')),
        ',"new_responsible":', toJSONString(coalesce(nullIf(trim(concat(u3.last_name, ' ', u3.first_name, ' ', u3.middle_name)), ''), 'null')), '}'
      ),
      'JSON'
    ),
    CAST(NULL, 'Nullable(JSON)')
  ) as responsible_change,
  -- toJSONString(NULL) → NULL; CH concat nulls whole string — create uses new_group only
  if(
    he.old_group_id is not null or he.new_group_id is not null,
    accurateCastOrNull(
      concat(
        '{"old_group":', coalesce(toJSONString(ug_old.name), 'null'),
        ',"new_group":', coalesce(toJSONString(ug_new.name), 'null'), '}'
      ),
      'JSON'
    ),
    CAST(NULL, 'Nullable(JSON)')
  ) as user_group_change,
  if(he.object_type in ('ticket_status', 'task_status'), he.status_comment, he.comment_status) as comment_status
from base as he
left join tickets as tk on he.ticket_id = tk.id
left join tasks as tsk_number on he.task_id = tsk_number.id
left join users as u on u.id = he.effective_user_id
left join statuses as st_old on st_old.id = he.old_status_id
left join statuses as st_new on st_new.id = he.new_status_id
left join statuses as st3 on st3.id = he.old_stage_id
left join statuses as st4 on st4.id = he.new_stage_id
left join users as u2 on u2.id = he.old_responsible_id
left join users as u3 on u3.id = he.new_responsible_id
left join user_groups as ug_old on ug_old.id = he.old_group_id
left join user_groups as ug_new on ug_new.id = he.new_group_id
