# Phase 3: PBX realtime webhooks (deferred)

## Why not in v1

Batch ingest already covers attribution:

- legs from `context_calls` + `members`
- `ext_params=1` → `conversion` block
- classic CSV `stats/request` (`from_extension` / `to_extension` / `disconnect_reason`)

Realtime adds earlier visibility and explicit transfer initiator / disconnect, but needs infra.

## Later requirements

1. Public HTTPS receiver (Airflow is pull-only today) with `sign` verification.
2. Bronze entity `call_events` (append-only) for:
   - `/events/call` (seq, location, transfer, disconnect_reason)
   - `/events/summary`
   - `/events/record/added` (optional recording pipeline)
3. dbt `int_pbx__call_events_current` + join to `cur_calls` on `entry_id`.

## Do not start until

- `cur_call_legs` / `cur_call_conversions` / `cur_call_stats_basic` are in BI and validated
- product confirms need for freshness < 1 hour vs hourly batch

