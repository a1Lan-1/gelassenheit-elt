"""Nightly FSD reconcile: full-refresh current models from all bronze (tag: reconcile)."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import FSD_RECONCILE_CRON_MSK, resolve_schedule

# cur_history is huge — only hourly ingest; not FR in nightly reconcile.
_RECONCILE_EXCLUDE = (
    "int_helpdesk__history_current int_helpdesk__history_current_v "
    "int_helpdesk__tickets_enriched_current int_helpdesk__tasks_enriched_current "
    "int_helpdesk__config_items_cashdesk int_helpdesk__task_ci_primary"
)


@dag(
    dag_id="dwh_reconcile_fsd",
    schedule=resolve_schedule(FSD_RECONCILE_CRON_MSK),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "helpdesk", "reconcile"],
    max_active_runs=1,
)
def reconcile_fsd():
    @task
    def build_reconcile_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        # tag:current_view = cur_*_v views; avoids dbt glob/+ operator parsing issues.
        return dbt_run_cmd(
            "tag:helpdesk,tag:current tag:helpdesk,tag:current_view",
            full_refresh=True,
            exclude=_RECONCILE_EXCLUDE,
        )

    wait_sync = dbt_sync_sensor()

    run_reconcile = SSHOperator(
        task_id="dbt_reconcile_fsd",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_reconcile_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    reconcile_cmd = build_reconcile_run()
    wait_sync >> reconcile_cmd >> run_reconcile


reconcile_fsd()
