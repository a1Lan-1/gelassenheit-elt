{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_ticket_actions_base',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'ticket_actions', 'ticket_actions_core', 'current'],
  )
}}

with history_extracted as (
  select *
  from {{ ref('int_helpdesk__history_action_extract') }}
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

history_derived as (
  select
    he.*,
    he.nv_comment as appeal_text,
    concat(he.nv_type, ' ', he.nv_source, ' ', he.nv_appeal_id) as appeal_info,
    multiIf(
      match(he.nv_value, '^[0-9]+$'),
      round(toFloat64(he.nv_value) / 3600, 1),
      CAST(NULL, 'Nullable(Float64)')
    ) as timer_value,
    he.nv_type as appeal_type,
    he.nv_source as appeal_source,
    he.nv_comment as status_comment,
    toUUIDOrNull(he.foreign_id) as foreign_uuid
  from history_extracted as he
),

batch_task_ids as (
  select distinct id
  from (
    select object_id as id from history_derived where object_type = 'tasks'
    union all
    select new_task_id as id from history_derived where object_type = 'task_status' and new_task_id is not null
    union all
    select foreign_uuid as id from history_derived where foreign_type = 'tasks' and foreign_uuid is not null
  )
  where id is not null
),

batch_ticket_status_ids as (
  select distinct object_id as id
  from history_derived
  where object_type = 'ticket_status'
),

batch_task_status_ids as (
  select distinct object_id as id
  from history_derived
  where object_type = 'task_status'
),

tasks as (
  select id, ticket_id, number
  from {{ ref('int_helpdesk__tasks_current') }} final
  where id in (select id from batch_task_ids)
),

ticket_status as (
  select id, ticket_id
  from {{ ref('int_helpdesk__ticket_status_current') }} final
  where id in (select id from batch_ticket_status_ids)
),

task_status as (
  select id, task_id
  from {{ ref('int_helpdesk__task_status_current') }} final
  where id in (select id from batch_task_status_ids)
)

select
  he.id as id,
  he.object_type as object_type,
  he.object_id as object_id,
  he.user_id as user_id,
  he.updated_at as updated_at,
  he.comment_text as comment_text,
  he.appeal_text as appeal_text,
  he.appeal_info as appeal_info,
  he.notify_user as notify_user,
  he.timer_value as timer_value,
  he.foreign_type as foreign_type,
  he.foreign_id as foreign_id,
  he.old_description as old_description,
  he.new_description as new_description,
  he.old_theme as old_theme,
  he.new_theme as new_theme,
  he.old_status_id as old_status_id,
  he.new_status_id as new_status_id,
  he.old_stage_id as old_stage_id,
  he.new_stage_id as new_stage_id,
  he.old_responsible_id as old_responsible_id,
  he.new_responsible_id as new_responsible_id,
  he.old_group_id as old_group_id,
  he.new_group_id as new_group_id,
  he.comment_status as comment_status,
  he.status_comment as status_comment,
  he.call_duration as call_duration,
  he.appeal_type as appeal_type,
  he.appeal_source as appeal_source,
  he.effective_user_id as effective_user_id,
  multiIf(
    he.object_type = 'ticket_status', coalesce(he.new_ticket_id, ts_tbl.ticket_id),
    he.object_type = 'tickets', he.object_id,
    he.object_type = 'task_status', coalesce(t_new.ticket_id, tsk.ticket_id),
    he.object_type = 'tasks', t_obj.ticket_id,
    he.foreign_type = 'tickets', he.foreign_uuid,
    he.foreign_type = 'tasks', t_for.ticket_id,
    CAST(NULL, 'Nullable(UUID)')
  ) as ticket_id,
  multiIf(
    he.object_type = 'task_status', coalesce(he.new_task_id, tskst.task_id),
    he.object_type = 'tasks', he.object_id,
    he.foreign_type = 'tasks', he.foreign_uuid,
    CAST(NULL, 'Nullable(UUID)')
  ) as task_id
from history_derived as he
left join tasks as t_obj
  on he.object_type = 'tasks' and t_obj.id = he.object_id
left join tasks as t_new
  on he.object_type = 'task_status' and t_new.id = he.new_task_id
left join ticket_status as ts_tbl
  on he.object_type = 'ticket_status' and ts_tbl.id = he.object_id
left join task_status as tskst
  on he.object_type = 'task_status' and tskst.id = he.object_id
left join tasks as tsk on tsk.id = tskst.task_id
left join tasks as t_for
  on he.foreign_type = 'tasks' and t_for.id = he.foreign_uuid
