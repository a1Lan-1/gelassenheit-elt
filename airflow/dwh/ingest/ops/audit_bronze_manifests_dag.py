"""Ops: optional backfill of bronze manifest audit tables (tag: audit_bronze).

Primary path: each ingest DAG inserts into sync_*.audit_bronze right after
uploading parquet+manifest to S3 (see dwh.common.ch_client).

This DAG is manual-only — use for gap repair / full-refresh rebuilds from S3
globs. Do not re-enable hourly schedule while live inserts are active (duplicates).
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor


@dag(
    dag_id="dwh_audit_bronze_manifests",
    schedule=None,
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "ops", "audit", "manual"],
    max_active_runs=1,
)
def audit_bronze_manifests():
    @task
    def build_audit_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:audit_bronze")

    wait_sync = dbt_sync_sensor()

    run_audit = SSHOperator(
        task_id="dbt_audit_bronze_manifests",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_audit_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    audit_cmd = build_audit_run()
    wait_sync >> audit_cmd >> run_audit


audit_bronze_manifests()
