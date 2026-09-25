# Append-only hourly snapshots.
# NEVER run with --full-refresh on this model (or tag:backlog_append_only) — that drops all history.
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='mart_tickets_backlog',
    engine=ch_engine_merge_tree(),
    order_by='(snapshot_hour, service_theme, user_group_name, current_status_name, new_channel)',
    partition_by='toYYYYMM(snapshot_hour)',
    settings={'allow_nullable_key': 1},
    pre_hook=[
      "{% if is_incremental() %}alter table {{ this }} delete where snapshot_hour = {{ fsd_backlog_snapshot_hour() }}{% endif %}"
    ],
    tags=['mart_fsd', 'helpdesk', 'backlog', 'backlog_append_only'],
  )
}}

select
  {{ fsd_backlog_snapshot_hour() }} as snapshot_hour,
  te.service_theme as service_theme,
  te.user_group_name as user_group_name,
  te.current_status_name as current_status_name,
  te.new_channel as new_channel,
  count() as ticket_count
from {{ ref('mart_helpdesk__tickets_enriched') }} as te
group by
  {{ fsd_backlog_snapshot_hour() }},
  te.service_theme,
  te.user_group_name,
  te.current_status_name,
  te.new_channel
