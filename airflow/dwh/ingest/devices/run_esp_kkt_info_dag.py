"""Daily ESP KKT info mart: product ClickHouse → gel_helpdesk (tag:kkt_info)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import ESP_KKT_INFO_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "esp_kkt_info.yaml"


def _kkt_info_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", ESP_KKT_INFO_CRON_MSK))


@dag(
    dag_id="dwh_run_esp_kkt_info",
    schedule=_kkt_info_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "esp", "marts", "kkt_info"],
    max_active_runs=1,
)
def run_esp_kkt_info():
    @task
    def build_kkt_info_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:kkt_info")

    @task
    def build_kkt_info_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("tag:kkt_info")

    wait_sync = dbt_sync_sensor()

    run_kkt_info = SSHOperator(
        task_id="dbt_run_esp_kkt_info",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_kkt_info_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    test_kkt_info = SSHOperator(
        task_id="dbt_test_esp_kkt_info",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_kkt_info_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_kkt_info_run()
    test_cmd = build_kkt_info_test()
    wait_sync >> run_cmd >> run_kkt_info >> test_cmd >> test_kkt_info


run_esp_kkt_info()
