"""GS marts: unified employee directory and other tag:mart_gs models."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.assets import GS_MART_OUTLETS, GS_MART_SCHEDULE_ASSETS
from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import GS_MARTS_CRON_MSK, hybrid_schedule, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "gs_marts.yaml"


def _mart_cron_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", GS_MARTS_CRON_MSK))


def _mart_schedule() -> object:
    return hybrid_schedule(_mart_cron_schedule(), GS_MART_SCHEDULE_ASSETS)


@dag(
    dag_id="dwh_run_gs_marts",
    schedule=_mart_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "gs", "marts"],
    max_active_runs=1,
)
def run_gs_marts():
    @task
    def build_marts_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:mart_gs")

    @task
    def build_marts_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("tag:mart_gs")

    wait_sync = dbt_sync_sensor()

    run_marts = SSHOperator(
        task_id="dbt_run_gs_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
        outlets=GS_MART_OUTLETS,
    )
    test_marts = SSHOperator(
        task_id="dbt_test_gs_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_marts_run()
    test_cmd = build_marts_test()
    wait_sync >> run_cmd >> run_marts >> test_cmd >> test_marts


run_gs_marts()
