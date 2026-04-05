{{
  config(
    materialized='table',
    alias='kkt_info',
    engine=ch_engine_merge_tree(),
    order_by='(cashdesk_id)',
    tags=['mart_esp', 'esp', 'kkt_info'],
  )
}}

{#
  Cashdesk-level snapshot of ESM/PMSR telemetry.
  - Grain: one row per cashdesk from mv_cashdesk.cashdesk
  - Version source: componentsReport only (no req_version fallback)
  - PMSR/TSP are nullable attributes, not row filters
  - Fleet filters (NORMAL / Windows / Linux) belong in the consumer query
#}

with events_typed as (
  select
    cashdesk_id::UInt64 as cashdesk_id,
    server_date,
    data.pmsr.name::String as pmsr_name,
    data.esm.esmVersion::String as esm_version,
    data.tsPiotInfo.tsPiotModel::String as tsp_model
  from stats.events_latest final
  where type = 'componentsReport'
    and has_error = false
    and cashdesk_id > 0
),

events_agg as (
  select
    cashdesk_id,
    argMax(pmsr_name, server_date) as pmsr_name,
    argMax(esm_version, server_date) as esm_version,
    argMax(tsp_model, server_date) as tsp_model,
    max(server_date) as last_report_date
  from events_typed
  group by cashdesk_id
)

select
  c.id::UInt64 as cashdesk_id,
  c.inn as inn,
  c.os as os,
  c.cashdesk_type as cashdesk_type,
  nullIf(a.pmsr_name, '') as pmsr_name,
  nullIf(a.esm_version, '') as esm_version,
  nullIf(a.tsp_model, '') as tsp_model,
  a.last_report_date as last_report_date
from mv_cashdesk.cashdesk as c final
left join events_agg as a on a.cashdesk_id = c.id::UInt64
where c.id > 0
