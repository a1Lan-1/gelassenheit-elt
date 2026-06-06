"""PBX marts: calls_detail + tag:mart_pbx (after dwh_sync_dbt_repo)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.assets import MANGO_MART_OUTLETS, MANGO_MART_SCHEDULE_ASSETS
from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import hybrid_schedule, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "pbx_marts.yaml"

# Detail + session events + aggregates; without separate legs/sip/conversions.
_MANGO_MARTS_SELECT = "int_pbx__calls_detail int_pbx__call_events tag:mart_pbx"


def _mart_cron_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", "20 * * * *"))


def _mart_schedule() -> object:
    return hybrid_schedule(_mart_cron_schedule(), MANGO_MART_SCHEDULE_ASSETS)


@dag(
    dag_id="dwh_run_pbx_marts",
    schedule=_mart_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "pbx", "marts"],
    max_active_runs=1,
)
def run_pbx_marts():
    @task
    def build_marts_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd(
            _MANGO_MARTS_SELECT,
            exclude="int_pbx__calls_current",
        )

    @task
    def build_marts_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("int_pbx__calls_detail int_pbx__call_events tag:mart_pbx")

    wait_sync = dbt_sync_sensor()

    run_marts = SSHOperator(
        task_id="dbt_run_pbx_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
        outlets=MANGO_MART_OUTLETS,
    )
    test_marts = SSHOperator(
        task_id="dbt_test_pbx_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    run_cmd = build_marts_run()
    test_cmd = build_marts_test()
    wait_sync >> run_cmd >> run_marts >> test_cmd >> test_marts


run_pbx_marts()
