# Local demo

## Prerequisites

- Docker Desktop / Docker Engine
- Python 3.11+
- (Optional) dbt-clickhouse for full model runs

## Bring up infra + synthetic data

```powershell
cd C:\project\gelassenheit-elt
copy .env.example .env
.\scripts\bootstrap\smoke.ps1
```

Or bash:

```bash
cp .env.example .env
bash scripts/bootstrap/smoke.sh
```

This starts ClickHouse, MinIO, Postgres; applies DDL; loads ~500 synthetic tickets and bot sessions.

## Optional dbt

```bash
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml   # adjust paths on Windows
pip install dbt-core dbt-clickhouse
dbt debug --profiles-dir .
# Full project needs bronze fixtures per entity; start with tags you care about:
# dbt run --select tag:mart_bot
```

## Services

| Service | URL |
|---------|-----|
| ClickHouse HTTP | http://localhost:8123 (`default` / `gel`) |
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 (gelassenheit / gelassenheit_secret) |
| Postgres | localhost:5432 (gel / gel / gel_sources) |
