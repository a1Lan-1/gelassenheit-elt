# Schedule audit (P0b) - Airflow hourly MSK

Sources: `dwh/common/schedules.py`, DAG factories, `BI_CUTOVER.md`.
Model baseline: `docs/speed_audit/BASELINE.md` (14d, `audit_dbt_runs`; `airflow_dag_id` often empty - DAG wall-clock is approximate).

## Minute-of-hour map (MSK)

| Min | DAG / group | Pool | Notes |
|----:|--------------|------|---------|
| :00 | `ingest_fsd_*`, `ingest_gs_*`, API hourly, ticket_geo | workers | Many parallel extracts; dbt current on some FSD right after bronze |
| :00-:20 | pbx/dialer ingest (API) | workers | |
| :20 | `dwh_run_pbx_marts` | `dbt_host` | detail+marts; hybrid cron+assets |
| :20 | `dwh_run_bot_marts` | `dbt_host` | **new**, competes with pbx on slots=1 |
| */20 | `dwh_run_dialer_marts` | `dbt_host` | :00/:20/:40 - frequent full-table rebuild |
| :45 | `dwh_fsd_hourly_ready` | - | gate |
| :54 | `dwh_run_fsd_marts` | `dbt_host` | SLA/OL/actions |
| :56 | `dwh_run_gs_marts` | `dbt_host` | daily totals |
| :57 | `dwh_run_esp_marts` | `dbt_host` | |
| :58 | `dwh_run_techsup_work_blocks` | `dbt_host` | |

Nightly/other: reconcile FSD `03:48`, cleanup orphans `02:00`, cashdesk geo `06:30`, ESP license `04:00`, ESP kkt `05:00`, yearly FR `01-01`.

`dbt_host` **slots=1** -> all SSH dbt strictly serial; overlapping cron = queue, not parallel.

## Actual vs slot (from 14d baseline)

Top p95 models (regular &lt;600s):

| Model | p50 | p95 | Slot comment |
|--------|----:|----:|---------------------|
| `int_devices__device_mark_events` | 30s | 125s | pulls ESP/marts; watch lookback |
| FSD `*_current` (many) | ~0.8s | 40-90s | bimodal: fast increment vs rare heavy runs |
| `int_helpdesk__ticket_lifecycle_events` | 17s | 44s | in :54 tail |
| `int_pbx__calls_detail` | 16s | 30s | fits :20-:45 with margin |
| `mart_workforce__employee_daily_totals` | 16s | 25s | :56 - ok on a single slot |

Tail **:54-:58** (4 DAGs back-to-back on slots=1): at p95 lifecycle~44s + enriched + GS daily~25s + esp + work_blocks easy to **spill into the next hour** or delay BI freshness past :00.

**Note (hybrid assets):** `dwh_run_fsd_marts` = cron `:54` **or** asset `fsd_hourly_ready` (:45). In practice marts often start **right after :45**, not waiting for :54 - hence CH peak ~:45-:47.

**Hard-cut:** FSD marts split into 3 SSH runs (actions -> enriched -> OL/marts), each `threads=1` - no parallel lifecycle+SLA+enriched in one dbt run.

Dialer `20 * * * *` + pbx `:20` + bot `:25`.

## Ops / CH load notes (query_log snapshot)

Snapshot "who loads the system" (user=`airflow`, window ~17:45-17:47):

| Model | wall | read | memory |
|--------|------|------|--------|
| `int_helpdesk__tickets_enriched_current` | ~12s | 1.6 GB | 1.3 GB |
| `int_helpdesk__ticket_lifecycle_events` | ~43s | **6.3 GB** | 807 MB |
| `int_helpdesk__task_ci_primary` | ~7s | 583 MB | **1.8 GB** |
| `int_helpdesk__tasks_enriched_current` | ~16s | 1.05 GB | 1.4 GB |
| `int_helpdesk__ticket_sla_current` | ~55s | 1.8 GB | (cut) |

Takeaway: end-of-hour spike is not random ingest but **FSD enriched -> lifecycle -> SLA** batch after hourly_ready. Main IO - `ticket_lifecycle_events` (6.3 GB read in ~40s). In parallel enriched/task_ci hold >=1 GB RAM each -> CH memory pressure even with `dbt_host slots=1` (dbt threads inside one run).

Next priority (on top of P1-P2): tighten lifecycle (less history/actions scan on affected tickets), do not raise threads on FSD marts, do not set slots=2 while this model cluster runs near :45.

## Optimality checklist

| Question | Verdict |
|--------|---------|
| Order ingest -> current -> marts? | Yes (FSD gate :45 -> marts :54) |
| Dialer too frequent? | **Yes**: full-table every 20 min excessive before incremental (P2); until P2 prefer `20 * * * *` or `40 * * * *` |
| :54-:58 tail cascades late? | **Risk yes** with busy `dbt_host` after pbx/sk; moving GS/esp/wb to :50/:52/:55 **not** before FSD marts - better shrink SQL (P1-P3) or slots=2 |
| Independent pbx parallel dialer serialized unnecessarily? | **Yes** at slots=1; after P2 (cheaper sk) - slots=2 for pbx/sk pair |
| Orchestrator :20 vs pbx :20 | Conflict; move bot to `:25` or `:35` |

## Recommendations (P4 rollout)

1. **Move** `dwh_run_bot_marts` -> `25 * * * *` (after pbx :20). Bot freshness risk: low.
2. **Reduce** dialer marts to `20 * * * *` (1x/hour) while detail is full-table; after incremental - restore `*/20` if desired.
3. **slots=2** on `dbt_host` only after CH CPU measurement: allow pbx parallel dialer; keep FSD tail serialized or separate pool.
4. **Do not move** :54 FSD marts before :45 ready.
5. **Populate** `airflow_dag_id` in `audit_dbt_runs` (often empty now) - otherwise DAG wall-clock cannot be measured; fix `log_dbt_run_stats` / dbt vars.
6. Leave as-is: yearly FR, reconcile 03:48, GS ingest :00.

## Freshness Metabase

Target FSD/GS KPI window: **by :58-:00**. If tail p95 &gt; ~4 min total on slot - BI sees the previous hour longer; P1 order_by + P3 lifecycle/daily totals beat cron shifts.
