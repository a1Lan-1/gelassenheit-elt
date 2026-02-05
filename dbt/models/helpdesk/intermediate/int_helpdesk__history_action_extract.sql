{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_history_action_extract',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'ticket_actions', 'ticket_actions_core', 'current'],
  )
}}

{# JSON path (col.key) only works on table columns — extract here once for actions. #}
select
  id as id,
  object_type as object_type,
  object_id as object_id,
  user_id as user_id,
  updated_at as updated_at,
  ifNull(toString(new_values.text), '') as comment_text,
  ifNull(toString(new_values.comment), '') as nv_comment,
  ifNull(toString(new_values.type), '') as nv_type,
  ifNull(toString(new_values.source), '') as nv_source,
  ifNull(toString(new_values.appeal_id), '') as nv_appeal_id,
  {{ fsd_json_notify_label('new_values') }} as notify_user,
  ifNull(toString(new_values.value), '') as nv_value,
  coalesce(
    nullIf(ifNull(toString(new_values.table_name), ''), ''),
    nullIf(ifNull(toString(new_values.foreign_table), ''), ''),
    object_type
  ) as foreign_type,
  coalesce(
    nullIf(ifNull(toString(new_values.foreign_id), ''), ''),
    nullIf(ifNull(toString(new_values.id), ''), '')
  ) as foreign_id,
  toUUIDOrNull(ifNull(toString(new_values.ticket_id), '')) as new_ticket_id,
  toUUIDOrNull(ifNull(toString(new_values.task_id), '')) as new_task_id,
  ifNull(toString(old_values.description), '') as old_description,
  ifNull(toString(new_values.description), '') as new_description,
  ifNull(toString(old_values.title), '') as old_theme,
  ifNull(toString(new_values.title), '') as new_theme,
  toUUIDOrNull(ifNull(toString(old_values.status_id), '')) as old_status_id,
  toUUIDOrNull(ifNull(toString(new_values.status_id), '')) as new_status_id,
  toUUIDOrNull(ifNull(toString(old_values.closure_code), '')) as old_stage_id,
  toUUIDOrNull(ifNull(toString(new_values.closure_code), '')) as new_stage_id,
  toUUIDOrNull(ifNull(toString(old_values.responsible_user_id), '')) as old_responsible_id,
  toUUIDOrNull(ifNull(toString(new_values.responsible_user_id), '')) as new_responsible_id,
  toUUIDOrNull(ifNull(toString(old_values.user_group_id), '')) as old_group_id,
  toUUIDOrNull(ifNull(toString(new_values.user_group_id), '')) as new_group_id,
  ifNull(toString(new_values.`ticket_status-comment`), '') as comment_status,
  toInt32OrNull(ifNull(toString(new_values.duration), '')) as call_duration,
  coalesce(user_id, toUUIDOrNull(ifNull(toString(new_values.user_id), ''))) as effective_user_id
from {{ ref('int_helpdesk__history_current') }}
{% if not is_incremental() %}
  final
{% endif %}
where object_type in ('appeals', 'tickets', 'tasks', 'comments', 'ticket_status', 'task_status')
{% if is_incremental() %}
  and updated_at > (
    select coalesce(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
    from {{ this }}
  )
{% endif %}
