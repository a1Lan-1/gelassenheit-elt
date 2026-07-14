# Airflow — DWH setup

## pool (required)

Admin → pools → create **`dbt_host`**, slots = **1**.

All `SSHOperator` with `ssh_conn_id= gel-dbt` use this pool to on `sd-anl-dbt00`
did not go in parallel with `dbt run`/ `git reset` / `dbt deps`.

## dbt sync + deps

Only Dag **`dwh_sync_dbt_repo`** (`*/5 * * * *`) executes` git fetch/reset `and` dbt deps`.
The other Dag's wait for him via task ** `wait_dbt_repo_sync` ** (`ExternalTaskSensor`).The dbt-host needs access to **hub.getdbt.com** (or mirror) for `dbt deps`.

## Connections

| Connection | Type | Purpose |
|------------|-----|------------|
| `gel-dbt` | SSH | sd-anl-dbt00, dbt-user → dbt run/test |
| `gelassenheit-dbt` | Git/SSH | pull repo `gelassenheit-dbt` on dbt-host |
| `gelassenheit-airflow` | Git/SSH | pull repo `gelassenheit-airflow` on Airflow |
| `fsd_prod_db` | Postgres | source FSD || `bot_ssh` | SSH | bastion, user `analyst` — tunnel to PG bot |
| `bot_ro` | Postgres | Host=`127.0.0.1`, Port=`5437`, DB=`bot`, user `analyst_ro`; Extra: `{"ssh_remote_host":"127.0.0.1","ssh_remote_port":5437}` |
| `nextcloud_calltrafic` | HTTP | Calltraffic Nextcloud WebDAV (xlsx timesheet) |

`nextcloud_calltrafic`: Host=`cloud.calltraffic.ru`, Login/Password = Nextcloud account
(better App password). Schema=`https`.On this, the NC WebDAV path goes through the * * user UUID * *, not through login:
`/remote.php/dav/files/0D810015-CF53-4FBB-80EB-779B9E685B80/...`
(the full path is set in `dwh/config/nc_entities.yaml→` `dav_path`).

Optional in Extra JSON:
```json
{"dav_user": "0D810015-CF53-4FBB-80EB-779B9E685B80"}
```
(only needed if there is a relative path in yaml without a UUID).

## Variables

| Key | Value |
|-----|-------|
| `dwh_s3_bucket` | `gel-lake` || `dwh_s3_endpoint` | `https://minio:9000` |
| 's3_key_id `| Garage Key ID |
| `s3_secret_key` | Garage Secret Key |
| `dwh_dbt_project_path` | `/home/dbt-user/dwh` |
| `dwh_dbt_target` | `prod` |

Runtime watermarks (auto): `dwh_watermark_fsd_{entity}`, `dwh_watermark_{domain}_{entity}`,
`dwh_watermark_fsd_ticket_geo` (ticket map; clear/empty → full lookback backfill),`dwh_watermark_cashdesk_geo` (cashdesk address map; clear/empty → full rebuild).

## Hardcoded

- ClickHouse: `clickhouse:8123` — `dwh/common/constants.py`
- CH user/password — `~/.env` on dbt-host
- Databases: `gel_helpdesk`, `gel_crm`, `gel_pbx`, `gel_dialer`, `gel_workforce`
  (bot current/marts → also `gel_helpdesk`)

## Deploy `dwh/` on Airflow

```bash
# On dev-machine: commit + push in gitlab (conn gelassenheit-airflow)
git add dwh/git commit -m "Add greenfield dwh/ ingest and dbt orchestration DAGs"
git push origin master

# On sd-airflow (or via gelassenheit-airflow git sync):
cd /path/to/airflow/dags && git pull
```

Verification: Dag `ingest_fsd_*`, `ingest_crm_deals`, `dwh_refresh_crm_token` appeared in the UI.

## DAGs (`dwh/` folder)

| Pattern | File | Example |
|---------|------|--------|
| FSD ingest factory | `dwh/ingest/fsd/ingest_fsd_dags.py` | `ingest_fsd_tickets` || Orchestrator ingest | `dwh/ingest/bot/ingest_bot_dags.py` | `ingest_bot_ai_bot_sessions`, `ingest_bot_fsd_items` |
| Orchestrator marts | `dwh/ingest/bot/run_bot_marts_dag.py` | `dwh_run_bot_marts` |
| API ingest | `dwh/ingest/api/build_api_dags.py` | `ingest_crm_deals` |
| CRM token | `dwh/ingest/crm/refresh_crm_token.py` | `dwh_refresh_crm_token` |

Configurations: `dwh/config/fsd_entities.yaml`, `api_entities.yaml`Existing product ClickHouse 'mv_* `tables are not materialized by this project.

## S3

Airflow directly uploads bronze files to Garage via the S3 API.
Credentials: Variables` s3_key_id `and` s3_secret_key `; endpoint:
`dwh_s3_endpoint`.

**Garage region: `esp`**

```bash
aws s3 ls s3://gel-lake --endpoint-url https://minio:9000 --region esp --no-verify-ssl
```

## Blocker: dbt-host → ClickHouseSee [`INFRA_FIREWALL.md'](INFRA_FIREWALL.md) — open TCP 8123 to `clickhouse`.

## BI cutover

See [`BI_CUTOVER.md`](BI_CUTOVER.md)