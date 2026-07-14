# DWH cutover checklist

The goal of cutover is to move consumers from legacy 'sd_analytics` to ClickHouse
layers of the new DWH without disabling legacy Dag until the reconciliation is complete.

## 1. Preparation

- Ensure that Airflow sees all new Dag's:

```bash
airflow dags list | grep -E 'ingest_fsd|ingest_crm|ingest_pbx|ingest_dialer|dwh_run_pbx_marts|dwh_run_dialer_marts|dwh_refresh_crm_token'
```

- Check for import errors by `dwh/`:

```bashairflow dags list-import-errors
```

- Check dbt-host access to ClickHouse:

```bash
curl -v http://clickhouse:8123/ping
```

## 2. Parallel run

Run new Dag's and parallel legacy:

1. FSD raw entities.
2. API entities: CRM, PBX, Dialer.
3. dbt current layer.
4. dbt marts.

Legacy Dag's remain on until the end of the reconciliation.

## 3. Reconciliation of raw/current

For each entity, check:

- source row count per window;- `audit_bronze_manifests.row_count`;
- staging row count;
- current row count;
- distinct primary keys in current.

Minimum list of FSD smoke entities:

- `tickets`
- `tasks`
- `history`
- `comments`
- `users`
- `statuses`
- `user_groups_mapping`

Minimum list of API smoke entities:

- `crm.deals`
- `crm.companies`
- `pbx.calls`
- `dialer.calls`
- `dialer.hours`

## 4. Reconciliation of marts

Compare to legacy 'sd_analytics` last 7/30 days:- The number of tickets
- the number of open tickets;
- number of tasks;
- the number of action history events;
- distribution by status;
- distribution among the responsible groups;
- number of PBX/Dialer calls;
- employment hours of managers.

## 5. BI switching

Switch consumers in groups:

1. FSD raw/current tables.
2. API current tables.
3. FSD marts.
4. Calls/work-hours marts.After each switch, leave the observation period and compare the key
metrics with legacy.

## 6. Rollback

Until the cutover rollback is complete, it is a return of the BI connection/query to the
legacy 'sd_analytics`. The data in the new ClickHouse is not deleted.

## 7. Criteria for completion

- All critical Dag's of the new DWH are green.
- `dbt test` passes for staging/current/marts.
- Row counts and key metrics converge with an acceptable deviation.- BI dashboards are powered by ClickHouse.
- Data owners have confirmed the correctness of the storefronts.