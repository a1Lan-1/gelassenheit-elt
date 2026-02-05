{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='int_lifecycle_actions_by_ticket',
    engine=ch_engine_replacing('updated_at'),
    order_by='(ticket_id, updated_at, id)',
    unique_key=['id'],
    partition_by='toYYYYMM(updated_at)',
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'operational_load', 'ticket_actions', 'ticket_actions_core', 'mart_fsd', 'current'],
  )
}}

{# Narrow layer for lifecycle/SLA: ORDER BY ticket_id + already retrieved status/group. #}
select
  id,
  ticket_id,
  ticket_number,
  user_name,
  updated_at,
  object_type,
  status_change,
  user_group_change,
  ifNull(JSONExtractString(toString(status_change), 'new_status'), '') as new_status,
  ifNull(JSONExtractString(toString(status_change), 'old_status'), '') as old_status,
  ifNull(JSONExtractString(toString(user_group_change), 'old_group'), '') as old_group,
  ifNull(JSONExtractString(toString(user_group_change), 'new_group'), '') as new_group
from {{ ref('mart_helpdesk__ticket_actions') }}
{% if not is_incremental() %}
final
{% endif %}
where object_type in ('tickets')
  and ticket_id is not null
  and (status_change is not null or user_group_change is not null)
  {% if is_incremental() %}
  and updated_at > (
    select coalesce(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
    from {{ this }}
  )
  {% endif %}
