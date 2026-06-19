"""Hourly bot marts: tag mart_bot."""

from __future__ import annotations

from datetime import datetime

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag, task

from dwh.common.assets import BOT_MART_OUTLETS
from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import resolve_schedule

BOT_MARTS_CRON_MSK = "25 * * * *"


@dag(
    dag_id="dwh_run_bot_marts",
    schedule=resolve_schedule(BOT_MARTS_CRON_MSK),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["dwh", "bot", "marts"],
    max_active_runs=1,
)
def run_bot_marts():
    @task
    def build_marts_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:mart_bot")

    @task
    def build_marts_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("tag:mart_bot")

    wait_sync = dbt_sync_sensor()

    run_marts = SSHOperator(
        task_id="dbt_run_bot_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
        outlets=BOT_MART_OUTLETS,
    )
    test_marts = SSHOperator(
        task_id="dbt_test_bot_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_marts_run()
    test_cmd = build_marts_test()
    wait_sync >> run_cmd >> run_marts >> test_cmd >> test_marts


run_bot_marts()
