# Support Bot — DWH schema

Source: Postgres `bot` @ `127.0.0.1:5437` via SSH `bastion`
(`analyst` / `analyst_ro`). Timestamptz in the source is **true UTC** (bronze/CH
stores naive MSK like the rest of the DWH).

CH target: **`gel_bot`** (dedicated bot domain database).
Bronze prefix remains `bot/` on S3.

## Extract host (phase 0)

| Host | TCP bastion:22 | SSH+PG |
|------|----------------|--------|
| Workstation (tunnel) | OK | OK |
| `dbt` (local/demo) | OK | OK (paramiko tunnel → 5437) |
| Airflow workers | network-dependent | — |

**Decision:** extract on an Airflow task with **SSH tunnel** (`bot_ssh` →
bastion, then `bot_ro` on `127.0.0.1:5437`). bastion:22 reachability should
match dbt; if AF workers cannot reach bastion — open routing or move
extract to dbt via `SSHOperator` (`gel-dbt`).

## Entities v1

| Entity | Table | PK | Watermark | Rows (≈) | Mode |
|--------|-------|-----|-----------|----------|------|
| `ai_bot_sessions` | `public.ai_bot_sessions` | `ticket_id` | `updated_at` | 19k | incremental + overlap 30m |
| `ai_answer_dialogs` | `public.ai_answer_dialogs` | `id` | `updated_at` | 460 | incremental + overlap 30m |
| `ai_usage_daily` | `public.ai_usage_daily` | `day` | — | 54 | **full snapshot** every run |
| `helpdesk_items` | `public.helpdesk_items` | `id` | `updated_at` | ~240k | incremental + overlap 30m |

DAG: `ingest_bot_<entity>` (factory `ingest_bot_dags.py`).

### `helpdesk_items` (bot ↔ Helpdesk mapping)

| Field | Meaning |
|-------|---------|
| `id` | bot item UUID (= `ai_bot_sessions.ticket_id`, `ai_answer_dialogs.item_id`) |
| `helpdesk_id` | Helpdesk ticket UUID (= `mart_tickets_enriched.id`) |
| `helpdesk_number` | ticket number |

## Local demo

Portfolio demo does not require SSH: Postgres in `docker-compose` plus synthetic
`gel_bot.demo_bot_sessions` / `gel_helpdesk.demo_tickets`.
