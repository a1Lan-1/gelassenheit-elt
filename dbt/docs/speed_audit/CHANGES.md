# Speed audit - deployed changes

Date: 2026-09-30. Plans: `dbt_pipeline_speed_audit` + `hard_cut_dbt_runtime`.

## P0 - baseline

- Artifacts: [`BASELINE.md`](BASELINE.md), [`baseline_p50_p95.json`](baseline_p50_p95.json)
- Script: `scripts/speed_audit_baseline_remote.py` (14d p50/p95)
- Full `bench_slow_models` run skipped (expensive on prod); baseline = `audit_dbt_runs`
- `airflow_dag_id` often empty -> DAG wall-clock unreliable; fix var propagation

## P0b - schedule

- [`SCHEDULE_AUDIT.md`](SCHEDULE_AUDIT.md)

## P1 - quick wins

- `order_by=(id)` for `history_action_extract`, `ticket_actions_base`, `mart_ticket_actions`
- Lookback defaults in `dbt_project.yml`: pbx hours=1, employee_daily=3, install_report=1
- Vars: `operational_load_lookback_days`, dialer/telephony/pbx_legs windows
- FINAL on all `int_workforce__*_current_v`

## P2 - full-rebuild to incremental

- `int_dialer__calls_detail` + dialer marts -> delete+insert + lookback
- `int_pbx__call_legs` / `call_leg_members` / `call_conversions` -> delete+insert + hours
- `int_telephony__calls_l12` -> delete+insert + days
- GS `*_current` kept as **table** (correct snapshot without stale keys); views with FINAL

## P3 - hot models (pre hard-cut)

- Top p95: `device_mark_events`, lifecycle, SLA - see hard-cut below
- **CH ops snapshot ~:45:** lifecycle **6.3 GB read** - cause: FINAL + mart view + threads=4

## P4 - orchestration

- Dialer marts: `20 * * * *`; bot `:25`
- `DBT_HOST_SLOTS_RECOMMENDED=2`, `DBT_HOST_SLOTS_ENABLED=1` (do **not** enable slots=2 before Wave F criteria)
- `dbt_run_cmd`: export `DWH_AIRFLOW_DAG_ID` / `RUN_ID`

---

## Hard-cut (all models) - Wave A-F

### Wave A - FSD marts orchestration

`gelassenheit-airflow` -> `dwh/ingest/fsd/run_fsd_marts_dag.py`:

1. `dbt_run_fsd_actions` - `tag:ticket_actions_core`, **threads=1**
2. `dbt_run_fsd_enriched` - `tag:tickets_enriched_core tag:tasks_enriched_core`, **threads=1**
3. `dbt_run_fsd_ol_marts` - `tag:mart_helpdesk` exclude cores+backlog, **threads=1**
4. backlog threads=2; test threads=2

Peak RAM from four heavy models in one run removed.

### Wave B - lifecycle / SLA / episodes

- `lifecycle_actions_by_ticket`: columns `new_status`/`old_status`/`old_group`/`new_group` (JSON once)
- `ticket_lifecycle_events`: no FINAL; `PREWHERE ticket_id` + `LIMIT 1 BY id`; tickets from `int_helpdesk__tickets_enriched_current`
- `ticket_sla_current`: single pass mart actions (touch) + resolve from lifecycle_actions; tickets from int
- `ticket_queue_episodes`: PREWHERE + spam filter via int (no mart FINAL)

### Wave C - enriched / task_ci

- Macros tickets/tasks enriched: dims from `*_current_v`; **id-first** without FINAL over full fact; tasks - prune tickets/CI/cashdesk/kkt/partners to batch keys
- `task_ci_primary`: `var('task_ci_primary_lookback_days')`; PREWHERE; CI only for affected `item_id`
- `mart_ticket_sla_daily`: lookback pushdown in CTE `sla` before UNIONs

### Wave D - ESP + table to incremental

- `device_mark_events`: lookback-window min + `least()` with prev (no full-history min)
- `ticket_first_status`: incremental append+RMT, new ticket_id only
- work_blocks events/daily: delete+insert by `work_date`, lookback `work_block_lookback_days=14`

### Wave E - bimodal `*_current`

Diagnosis: `order_by=(id)` already fine; no OPTIMIZE in ingest; p95 tail = large bronze parquet at `DEFAULT_BATCH_SIZE=100k`.

Fix: `batch_size: 25000` for `config_items`, `tickets`, `comments`, `tasks`, `places` (+ already on `history`) in `fsd_entities.yaml`.

### Wave F - verification / slots

**After deploy (required):**

1. One-time FR: `int_helpdesk__lifecycle_actions_by_ticket` (new columns), then lifecycle+SLA if needed
2. One-time FR: `int_devices__device_mark_events` (incremental semantics change)
3. First run work_blocks / ticket_first_status after strategy change (or `--full-refresh`)
4. Control hour + query_log: lifecycle read **&lt; 2 GB**, p95 lifecycle **&lt; 25s**
5. `scripts/speed_audit_baseline_remote.py` after 1-7d; refresh BASELINE
6. Success criteria -> only then `DBT_HOST_SLOTS_ENABLED=2` (pbx parallel sk); FSD tail serialized

**slots=2 now: no** (`DBT_HOST_SLOTS_ENABLED=1`).

## Post-deploy (P2 + hard-cut summary)

1. dialer detail/marts, pbx legs*, telephony l12 - first run / FR
2. FR `lifecycle_actions_by_ticket` + OL chain if needed
3. FR `device_mark_events`
4. Re-run baseline; decide slots=2 per Wave F criteria
