# Firewall: sd-anl-dbt00 → ClickHouse analytics

## Problem

`dbt debug` fails on **sd-anl-dbt00** with connect timeout to **clickhouse:8123**.

Verified from dbt-host:

```bash
getent hosts clickhouse
curl --connect-timeout 5 "http://clickhouse:8123/?query=SELECT%201"  # timeout if firewall blocks
```

Ports **8123, 8443, 9000, 9440, 443** — all timeout from dbt-host.

S3/Garage from the same host works (`aws s3 ls s3://gel-lake` OK).

## ClickHouse → S3 (bronze reads in `s3_parquet_run`)

`dbt run` sends SQL to **s-ch00**; the `s3()` table function runs **on the ClickHouse server**, not on sd-anl-dbt00.
Ingest can succeed (Airflow → Garage) while dbt fails with `S3_ERROR` / HTTP 400 if CH cannot read the same object.

| Source | Destination | Port | Protocol |
|--------|-------------|------|----------|
| clickhouse | minio:9000 | 443 | TCP HTTPS |

On **sd-anl-dbt00**, `~/.env` must define `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (same keys as bronze upload). dbt embeds them into the query sent to CH.

### Verify bronze object exists (dbt-host)

```bash
source ~/.env
aws s3 ls "s3://gel-lake/bronze/fsd/tickets/dt=2026-06-04/run_id=YOUR_RUN_ID/" \
  --endpoint-url https://minio:9000 --region esp --no-verify-ssl
```

### Probe read via ClickHouse (executed on s-ch00)

Replace `RUN_ID`, keys from `~/.env`:

```bash
curl -sS "http://clickhouse:8123/" \
  --user "${DWH_CH_USER}:${DWH_CH_PASSWORD}" \
  --data-binary "SELECT count() FROM s3(
    'https://minio:9000/gel-lake/bronze/fsd/tickets/dt=2026-06-04/run_id=RUN_ID/*.parquet',
    '${AWS_ACCESS_KEY_ID}',
    '${AWS_SECRET_ACCESS_KEY}',
    'Parquet'
  )"
```

ClickHouse against Garage rejects a **single-object** URL (`.../data.parquet`, HTTP 400); use a **glob** (`.../run_id=RUN_ID/*.parquet`). If glob fails while `aws s3 ls` works on dbt-host → fix **CH → Garage** network or credentials on CH side.

## Required rule

Allow **egress** from dbt-host to analytics ClickHouse:

| Source | Destination | Port | Protocol |
|--------|-------------|------|----------|
| dbt | clickhouse | 8123 | TCP HTTP |

## Verification (after firewall change)

On sd-anl-dbt00 as `dbt-user`:

```bash
curl -sS "http://clickhouse:8123/?query=SELECT%201"
cd ~/dwh && source venv/bin/activate && set -a && source ~/.env && set +a && dbt debug --target prod
```

Or from workstation:

```bash
export DBT_USER_PASSWORD='...'
python gelassenheit-dbt/scripts/check_infra.py
```

Expected: `Connection test: [OK]`, all checks green.

## Existing product ClickHouse tables

Product ClickHouse `mv_*` tables already exist and are not recreated by this project.
No `remote()` dbt models or ESP mirror DAGs are part of the new DWH build.
