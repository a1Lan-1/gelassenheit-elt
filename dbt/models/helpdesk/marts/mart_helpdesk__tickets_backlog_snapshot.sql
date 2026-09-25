-- Hourly full backlog snapshot: one row per MSK hour, tickets packed into JSON (disk-efficient).
-- NEVER --full-refresh (tag:backlog_append_only) — drops all history.
{{
  config(
    enabled=false,
    materialized='incremental',
    incremental_strategy='append',
    alias='mart_tickets_backlog_snapshot',
    engine=ch_engine_merge_tree(),
    order_by='(snapshot_hour)',
    partition_by='toYYYYMM(snapshot_hour)',
    settings={'allow_nullable_key': 1},
    pre_hook=[
      "{% if is_incremental() %}alter table {{ this }} delete where snapshot_hour = {{ fsd_backlog_snapshot_hour() }}{% endif %}"
    ],
    post_hook=[
      "ALTER TABLE {{ this }} MODIFY TTL snapshot_hour + toIntervalMonth(18)"
    ] if not is_incremental() else [],
    tags=['mart_fsd', 'helpdesk', 'backlog', 'backlog_snapshot', 'backlog_append_only'],
  )
}}

select
  {{ fsd_backlog_snapshot_hour() }} as snapshot_hour,
  now64(3) as snapshot_at,
  toUInt32(count()) as ticket_count,
  toJSONString(
    groupArray(
      map(
        'ticket_id', toString(te.id),
        'number', coalesce(te.number, ''),
        'current_status_name', coalesce(te.current_status_name, ''),
        'service_theme', coalesce(te.service_theme, ''),
        'user_group_name', coalesce(te.user_group_name, ''),
        'new_channel', coalesce(te.new_channel, ''),
        'service_name', coalesce(te.service_name, ''),
        'responsible_user_full_name', coalesce(te.responsible_user_full_name, ''),
        'updated_at', toString(te.updated_at)
      )
    )
  ) as tickets_json
-- Kept as String: existing append-only history is String; ALTER→JSON failed on prod.
-- New rows stay compatible; consumers use JSONExtractArrayRaw(tickets_json).
from {{ ref('mart_helpdesk__tickets_enriched') }} as te
