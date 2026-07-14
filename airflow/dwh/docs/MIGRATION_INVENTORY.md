# Legacy DAG migration matrix

Statuses:

- `covered` — logic moved to the new Airflow/dbt stack.
- `partial` — config or model exists, but not all legacy behavior is ported.
- `planned` — separate migration phase.
- `external` — source stays outside the new DWH.
- `deferred` — explicitly out of current scope.

**Ingest (FSD):** bronze → `int_helpdesk__{entity}_current` (RMT) → **`cur_{alias}_v` (FINAL)**.  
Incremental without overlap; hourly at :00 MSK (`0 * * * *`), timezone `Europe/Moscow`.

## FSD raw ingest

| Legacy DAG/file | Source | Legacy target | New DAG | New dbt model | Schedule | Status | Notes |
|---|---|---|---|---|---|---|---|
| `etl_sd/*_migrate_pipeline.py` (all entities) | `public.*` | `delta.support_service_desk_*` | `ingest_fsd_{entity}` | `int_helpdesk__{entity}_current` + `cur_*_v` | YAML / MSK | `covered` | Strict watermark, no 3h overlap |
| `etl_sd/history_migrate_pipeline.py` | `public.history` | `delta.support_service_desk_history` | `ingest_fsd_history` | `int_helpdesk__history_current` | hourly / MSK | `covered` | Marts: `mart_helpdesk__ticket_actions*` |

## API ingest

| Legacy DAG/file | Source | Legacy target | New DAG | New dbt model | Status | Notes |
|---|---|---|---|---|---|---|
| `crm/*` | CRM API | `crm.*` | `ingest_crm_*` | `int_crm__*_current` | `covered` | |
| `pbx/pbx_api_calls.py` | PBX API | `calls.support_pbx_calls` | `ingest_pbx_calls` | `int_pbx__calls_current` | `covered` | |
| `etl_dialer/esp_scorozvon_*.py` | Dialer API | `calls.*` | `ingest_dialer_*` | `int_dialer__*_current` | `covered` | |
| `crm/refresh_amo_token.py` | CRM OAuth | Variables | `dwh_refresh_crm_token` | n/a | `covered` | |
| `etl_dialer/dialer_key_update.py` | Dialer OAuth | Variables | `dwh_refresh_dialer_token` | n/a | `covered` | `@hourly` |

## FSD marts (phase 2)

| Legacy DAG/file | Legacy target | New dbt model | New DAG | Status |
|---|---|---|---|---|
| `support_analytics/1.py` | `delta.support_service_desk_tickets_enriched` | `mart_helpdesk__tickets_enriched` | `dwh_run_fsd_marts` | `covered` |
| `support_analytics/create_ticket_actions_history.py` | `delta.support_sd_ticket_actions` | `mart_helpdesk__ticket_actions` | `dwh_run_fsd_marts` | `covered` |
| `etl_sd/tickets_backlog_monitoring.py` | `delta.backlog_monitoring_tickets` | `mart_helpdesk__tickets_backlog` | `dwh_run_fsd_marts` | `covered` |
| `etl_sd/tasks_backlog_monitoring.py` | `delta.backlog_monitoring_tasks_n` | `mart_helpdesk__tasks_backlog` | `dwh_run_fsd_marts` | `covered` |

Marts schedule: `54 * * * *` MSK (`dwh/config/fsd_marts.yaml`), after ingest (:00); reconcile — 03:48 MSK daily.

ESP marts: `57 * * * *` MSK (`dwh/config/esp_marts.yaml`).

## Ops (phase 2)

| Legacy / component | New DAG / model | Status |
|---|---|---|
| Bronze manifest audit | `dwh_audit_bronze_manifests`, `tag:audit_bronze` → `{sync_*}.audit_bronze` (4 tables) | `covered` |
| Duplicate PK on `cur_*` | `ops_cur_duplicate_keys`, `dwh_ops_cur_duplicate_keys` | `covered` |
| dbt orphan tables cleanup | `dwh_ops_cleanup_dbt_orphans` (+ `post_hook` `drop_dbt_tmp_suffix`) | `covered` |
| `airflow_health_monitor.py` | `dwh_airflow_health_monitor` (filter `ingest_*`, `dwh_*`) | `covered` |

## Out of scope / deferred

| Legacy | New location | Status |
|---|---|---|
| `g_sheets/employee_hours.py`, `work_hours_from_gs` | — | `deferred` |
| `etl_esp/*.py` | product ClickHouse | `external` |
| `etl_dialer/esp_scorozvon_mv_refresh` | — | `deferred` |
| Reconcile full bronze | `dwh_reconcile_fsd` 03:48 MSK daily (full-refresh current+views) | `covered` |
| Pause legacy marts DAG | after reconciliation in `CUTOVER_CHECKLIST` §4 | `planned` |

