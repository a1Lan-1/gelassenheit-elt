# PBX API expand — notes

## Sample (2026-08-27): `gel_pbx.cur_calls_v` last 20 calls

- conversion / tags / marks: no block in the response with `ext_params=1` (contact center likely unused on the account). Showcase columns stay nullable.
- cost / context_cost_*: API returns no keys even with `ext_fields`.
- recording_ids: built from `context_calls[].recording_id` (+ members); top-level is almost always empty.
- Leg keys include: `call_type`, `call_abonent_id`, `call_abonent_info`, `call_abonent_extension`,
  `call_abonent_number` (on members), `call_answer_time`, `call_end_reason`, transfer flags, `members[]`.
- Group legs often answer via `members[]` with `call_type=user` and SIP `call_abonent_number`.

See `pbx_api_sample_notes.json` next to this file.

## Ingest changes (v1)

1. `stats/calls/request` → `ext_params=1` (+ cost `ext_fields`). `conversion` / tags / marks stay in `raw_data` (no new bronze flat columns — old parquet compatible).
2. Entity `call_stats_basic` from classic `stats/request` CSV.
3. CC/IO snapshots: `deals`, `tasks`, `dialer_campaigns`, `dialer_tasks`.

## Airflow / dbt

| DAG | Role |
|-----|------|
| `ingest_pbx_*` | bronze → `cur_*` / `cur_*_v` |
| **`dwh_run_pbx_marts`** | **`cur_calls_detail`**, **`cur_call_events`**, `tag:mart_pbx` (excludes legs/sip) |

Marts schedule: cron `20 * * * *` and/or assets `cur_calls_v` & `cur_users_v` & `cur_groups_v`.

## Grain: detail vs session events (vs Dialer)

| Layer | Grain | Purpose |
|------|-------|---------|
| **`cur_calls_detail`** | `(entry_id, dialog_seq)` | Hop-level: each answering agent = one row. Operator AHT/load. |
| **`cur_call_events`** | `(entry_id, event_seq)` | Dialer-like: `incoming` / `transfer` / `missed` without consult-bridge hop. Summary KPI. |
| **Dialer `cur_calls_detail`** | `id` | One row = one API record; `call_type_code` from API, dbt does not split legs. |

### Consult transfer = 3 hops in detail, 2 events in session

1. **hop1 / event incoming** — L1 agent answers the customer.
2. **hop2 / event transfer** — real L1→L2 transfer (`is_real_transfer_hop=1`, `transfer_from≠transfer_to`).
3. **hop3** — **consult-bridge** (`is_consult_bridge_leg=1`): same L2 agent, conversation continues; **not** a second transfer.

### Wait (`wait_to_accept_sec` / AWT)

- **hop1 (incoming/outgoing):** only `started_at→accept` (ring). `dial_duration` on user-leg in group-hunt = full leg length minus talk, **do not** use for AWT.
- **seq>1:** `max(dial, start→accept, max(0, prev_end→accept))`.
  On consult `prev_end` is often **after** accept (overlapping legs) → negative dateDiff;
  transfer AWT effectively = **dial** (3–11s in tests). Hop→bridge gap (0–4s) is **not** added to transfer AWT.
- Transfer AWT KPI and transfer marts → only `is_real_transfer_hop` + `wait_to_accept_sec`.

### ASA (`asa_sec`)

- Average Speed of Answer: **queue entry to answer** (answered only, else NULL).
- **hop1:** `entry_started_at → employee_accepted_at`.
- **transfer (seq>1):** = `wait_to_accept_sec` (wait until accept on target queue/stage).

### AHT (`aht_talk_duration`)

- Consult-bridge is **not** a separate AHT unit (`aht_talk_duration IS NULL`).
- On real transfer hop: `talk(hop) + talk(next bridge of same user)` — one logical L2 conversation.
- Blind without bridge: hop talk only.

## Phase 3 — Realtime (deferred)

Not in v1. Requires:

- Public HTTPS endpoint accepting signed PBX webhooks:
  `/events/call`, `/events/summary`, `/events/recording`, `/events/record/added`, …
- Bronze entity `call_events` (append-only) keyed by `(entry_id, call_id, seq)` / event type.
- Join to `cur_calls` for live disconnect_reason / transfer_initiator beyond batch legs.

Do **after** batch legs + conversions + classic CSV are stable in BI.

## Out of v1

- Recording download / speech analytics storage.
- Live agent presence polling (unless CC exposes a batch history list).

## One-shot Dialer → pbx bronze — cancelled

One-off `sz_inject_*` was rolled back (`--purge --reload`): objects removed from lake, `cur_calls` / detail without `sz-*`.

Script (if needed again): `gelassenheit-dbt/scripts/inject_dialer_calls_as_pbx_bronze.py`

