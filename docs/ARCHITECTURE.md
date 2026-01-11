# Architecture — Gelassenheit ELT

## Intent

Portfolio demonstration of a production-style analytics platform:

1. **Extract** from operational systems (Helpdesk PG, Bot PG, PBX/Dialer APIs, Workforce files)
2. **Load** immutable **bronze** parquet into object storage (`gel-lake`)
3. **Transform** with **dbt on ClickHouse** into current (ReplacingMergeTree) + mart layers
4. **Serve** analytics via ClickHouse marts (any SQL / BI client)

## Monorepo layout

| Path | Role |
|------|------|
| `airflow/` | Orchestration package (DAG factories, ingest, pools) |
| `dbt/` | Transform project (models, macros, tests) |
| `fixtures/` | Synthetic SQL + generated demo data |
| `docker-compose.yml` | Local ClickHouse, MinIO, Postgres |
| `scripts/` | Scrub, fixture generation, bootstrap |

## Domain databases

See [SCHEMAS.md](SCHEMAS.md). Helpdesk, Devices, and Bot are **separate** ClickHouse databases (cleaner than the original mixed layout).

## Incremental patterns (what this repo showcases)

- Bronze append by `dt` + `run_id`
- `*_current` entities: append + ReplacingMergeTree + `*_v` FINAL views
- Hot marts: `delete+insert` with lookback windows (SLA, lifecycle, telephony detail)
- Ops audit tables for run duration (p50/p95 speed audit methodology)

## Local vs production

Local demo focuses on **reproducible smoke**: CH databases + synthetic tickets/bot sessions.  
Full Airflow→API extract is stubbed where live vendor APIs cannot ship under NDA; interfaces remain in `airflow/dwh/ingest/*`.
