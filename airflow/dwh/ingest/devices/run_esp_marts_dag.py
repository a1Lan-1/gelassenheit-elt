"""Hourly ESP install report: product ClickHouse → gel_helpdesk (tag:install_report)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import ESP_MARTS_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "esp_marts.yaml"


def _mart_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", ESP_MARTS_CRON_MSK))


@dag(
    dag_id="dwh_run_esp_marts",
    schedule=_mart_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "esp", "marts"],
    max_active_runs=1,
)
def run_esp_marts():
    @task
    def build_marts_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:install_report")

    @task
    def build_marts_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("tag:install_report")

    wait_sync = dbt_sync_sensor()

    run_marts = SSHOperator(
        task_id="dbt_run_esp_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    test_marts = SSHOperator(
        task_id="dbt_test_esp_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_marts_run()
    test_cmd = build_marts_test()
    wait_sync >> run_cmd >> run_marts >> test_cmd >> test_marts


run_esp_marts()
