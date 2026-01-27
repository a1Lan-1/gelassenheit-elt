{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='stg_employee_work_history_events',
    engine=ch_engine_merge_tree(),
    order_by='(work_date, user_id, event_at)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'work_blocks', 'staging'],
  )
}}

select
  h.user_id as user_id,
  h.updated_at as event_at,
  toDate(h.updated_at) as work_date
from {{ ref('int_helpdesk__history_current_v') }} as h
where h.user_id is not null
  and h.deleted_at is null
{% if is_incremental() %}
  and h.updated_at > (
    select coalesce(max(event_at), toDateTime64('1970-01-01 00:00:00', 3))
    from {{ this }}
  )
{% endif %}
