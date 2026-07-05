"""Yearly full-refresh of every dbt model in gelassenheit-dbt (all sync_* tables)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import (
    DBT_SSH_POOL,
    SSH_CONN_DBT,
    SSH_DBT_YEARLY_FULL_REFRESH_TIMEOUT,
)
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import DBT_YEARLY_FULL_REFRESH_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "dbt_yearly_full_refresh.yaml"


def _yearly_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", DBT_YEARLY_FULL_REFRESH_CRON_MSK))


def _yearly_exclude() -> str | None:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    items = cfg.get("exclude") or []
    if not items:
        return None
    return " ".join(items)


@dag(
    dag_id="dwh_ops_dbt_yearly_full_refresh",
    schedule=_yearly_schedule(),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dwh", "dbt", "ops", "yearly", "full_refresh"],
    max_active_runs=1,
    doc_md="""
    Once per year (1 January, 02:00 MSK): `dbt run --full-refresh --target prod` for persistent
    models (current, marts, ESP). Skips `tag:staging`/`tag:debug` and FSD history.

    Waits for `dwh_sync_dbt_repo` so the dbt host has the latest code before the run.
    Uses pool `dbt_host` (single slot) and an extended SSH timeout (8h).
    """,
)
def ops_dbt_yearly_full_refresh():
    @task
    def build_full_refresh_run() -> str:
        from dwh.common.ssh import dbt_run_all_cmd

        return dbt_run_all_cmd(full_refresh=True, exclude=_yearly_exclude())

    wait_sync = dbt_sync_sensor()

    run_full_refresh = SSHOperator(
        task_id="dbt_yearly_full_refresh_all",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_full_refresh_run') }}",
        cmd_timeout=SSH_DBT_YEARLY_FULL_REFRESH_TIMEOUT,
    )

    refresh_cmd = build_full_refresh_run()
    wait_sync >> refresh_cmd >> run_full_refresh


ops_dbt_yearly_full_refresh()
