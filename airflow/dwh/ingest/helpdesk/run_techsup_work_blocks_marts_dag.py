"""Support employee work blocks: dbt tag:work_blocks (history + calls + schedule)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.assets import TECHSUP_WORK_BLOCKS_MART_OUTLETS, TECHSUP_WORK_BLOCKS_SCHEDULE_ASSETS
from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import TECHSUP_WORK_BLOCKS_CRON_MSK, hybrid_schedule, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "techsup_work_blocks_marts.yaml"


def _load_cfg() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _mart_cron_schedule() -> object:
    return resolve_schedule(_load_cfg().get("schedule", TECHSUP_WORK_BLOCKS_CRON_MSK))


def _mart_schedule() -> object:
    return hybrid_schedule(_mart_cron_schedule(), TECHSUP_WORK_BLOCKS_SCHEDULE_ASSETS)


@dag(
    dag_id="dwh_run_techsup_work_blocks",
    schedule=_mart_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "helpdesk", "techsup", "work_blocks", "marts"],
    max_active_runs=1,
)
def run_techsup_work_blocks():
    cfg = _load_cfg()
    dbt_select = cfg.get("dbt_select", "tag:work_blocks")

    @task
    def build_run_cmd() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd(dbt_select)

    @task
    def build_test_cmd() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd(dbt_select)

    wait_sync = dbt_sync_sensor()

    run_marts = SSHOperator(
        task_id="dbt_run_work_blocks",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_run_cmd') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
        outlets=TECHSUP_WORK_BLOCKS_MART_OUTLETS,
    )
    test_marts = SSHOperator(
        task_id="dbt_test_work_blocks",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_test_cmd') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_run_cmd()
    test_cmd = build_test_cmd()
    wait_sync >> run_cmd >> run_marts >> test_cmd >> test_marts


run_techsup_work_blocks()
