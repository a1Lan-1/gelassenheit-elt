# Gelassenheit ELT

Open-source **demonstration** of an end-to-end analytics ELT platform I designed and built:

**sources → bronze (object storage) → dbt (ClickHouse) → marts**

This repository is a **fully anonymized** reconstruction of a production-grade pipeline for portfolio use.  
No employer infrastructure, credentials, or proprietary business dictionaries are included.

## Stack

| Layer | Tech |
|-------|------|
| Orchestration | Apache Airflow |
| Transform | dbt + ClickHouse |
| Lake | MinIO (S3-compatible) |
| Sources (demo) | Postgres mocks + API stubs |
| Consume | ClickHouse marts (any BI / SQL client) |

## Domains (ClickHouse databases)

| Database | Domain |
|----------|--------|
| `gel_helpdesk` | Tickets, lifecycle, SLA, queue episodes |
| `gel_devices` | Device / install / product ops marts |
| `gel_bot` | Support AI-bot sessions & coverage |
| `gel_crm` | CRM entities |
| `gel_pbx` | Cloud PBX calls |
| `gel_dialer` | Outbound dialer + unified telephony |
| `gel_workforce` | Workforce / employee daily totals |

## Quickstart

```bash
cd gelassenheit-elt
cp .env.example .env
docker compose up -d
python scripts/generate_fixtures.py
scripts/bootstrap/smoke.sh   # or smoke.ps1 on Windows
```

See [docs/LOCAL_DEMO.md](docs/LOCAL_DEMO.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).


