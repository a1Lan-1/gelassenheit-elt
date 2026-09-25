# Append-only hourly snapshots from mart_tasks_enriched.
# NEVER run with --full-refresh on this model (or tag:backlog_append_only) — that drops all history.
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='mart_tasks_backlog',
    engine=ch_engine_merge_tree(),
    order_by='(snapshot_hour, status_name, user_group_name, vendor, partner_code)',
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
  te.status_name as status_name,
  te.user_group_name as user_group_name,
  te.vendor as vendor,
  te.partner_code as partner_code,
  te.partner_name as partner_name,
  count() as tasks_count
from {{ ref('mart_helpdesk__tasks_enriched') }} as te
group by
  {{ fsd_backlog_snapshot_hour() }},
  te.status_name,
  te.user_group_name,
  te.vendor,
  te.partner_code,
  te.partner_name
