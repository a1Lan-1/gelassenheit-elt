"""Daily ESP license funnel marts: product ClickHouse → gel_helpdesk (tag:license_funnel)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import ESP_LICENSE_FUNNEL_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "esp_license_funnel.yaml"


def _funnel_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", ESP_LICENSE_FUNNEL_CRON_MSK))


@dag(
    dag_id="dwh_run_esp_license_funnel",
    schedule=_funnel_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "esp", "marts", "license_funnel"],
    max_active_runs=1,
)
def run_esp_license_funnel():
    @task
    def build_funnel_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:license_funnel")

    @task
    def build_funnel_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("tag:license_funnel")

    wait_sync = dbt_sync_sensor()

    run_funnel = SSHOperator(
        task_id="dbt_run_esp_license_funnel",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_funnel_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    test_funnel = SSHOperator(
        task_id="dbt_test_esp_license_funnel",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_funnel_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_funnel_run()
    test_cmd = build_funnel_test()
    wait_sync >> run_cmd >> run_funnel >> test_cmd >> test_funnel


run_esp_license_funnel()
