"""Yearly Dialer full-refresh: current + views + calls_detail + marts."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import SKOROZVON_YEARLY_FULL_REFRESH_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "dialer_yearly.yaml"


def _yearly_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", SKOROZVON_YEARLY_FULL_REFRESH_CRON_MSK))


@dag(
    dag_id="dwh_dialer_yearly_full_refresh",
    schedule=_yearly_schedule(),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dwh", "dialer", "reconcile", "yearly"],
    max_active_runs=1,
)
def reconcile_dialer_yearly():
    @task
    def build_yearly_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd(
            "tag:dialer,tag:current tag:dialer,tag:current_view "
            "int_dialer__calls_detail tag:mart_dialer",
            full_refresh=True,
        )

    wait_sync = dbt_sync_sensor()

    run_yearly = SSHOperator(
        task_id="dbt_dialer_yearly_full_refresh",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_yearly_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    yearly_cmd = build_yearly_run()
    wait_sync >> yearly_cmd >> run_yearly


reconcile_dialer_yearly()
