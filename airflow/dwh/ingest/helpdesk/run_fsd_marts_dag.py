"""Hourly FSD marts: run and test models tagged mart_fsd.

Three sequential dbt-run steps (threads=1) — avoid parallel CH load from
lifecycle/enriched/SLA in one run with threads=4.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag, task

from dwh.common.assets import FSD_MART_OUTLETS, FSD_MART_SCHEDULE_ASSETS
from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.schedules import FSD_MARTS_CRON_MSK, hybrid_schedule, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "fsd_marts.yaml"

# The action / enriched kernel is already in separate steps — do not duplicate in mart_fsd.
_MART_OL_EXCLUDE = (
    "tag:backlog_append_only "
    "tag:ticket_actions_core "
    "tag:tickets_enriched_core "
    "tag:tasks_enriched_core"
)


def _mart_cron_schedule() -> object:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    return resolve_schedule(cfg.get("schedule", FSD_MARTS_CRON_MSK))


def _mart_schedule() -> object:
    return hybrid_schedule(_mart_cron_schedule(), FSD_MART_SCHEDULE_ASSETS)


@dag(
    dag_id="dwh_run_fsd_marts",
    schedule=_mart_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "helpdesk", "marts"],
    max_active_runs=1,
)
def run_fsd_marts():
    @task
    def build_actions_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd("tag:ticket_actions_core", threads=1)

    @task
    def build_enriched_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd(
            "tag:tickets_enriched_core tag:tasks_enriched_core",
            threads=1,
        )

    @task
    def build_ol_marts_run() -> str:
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd(
            "tag:mart_helpdesk",
            exclude=_MART_OL_EXCLUDE,
            threads=1,
        )

    @task
    def build_backlog_run() -> str:
        from dwh.common.airflow_context import msk_snapshot_hour_str
        from dwh.common.ssh import dbt_run_cmd

        return dbt_run_cmd(
            "tag:backlog_append_only",
            backlog_snapshot_hour=msk_snapshot_hour_str(),
            threads=2,
        )

    @task
    def build_marts_test() -> str:
        from dwh.common.ssh import dbt_test_cmd

        return dbt_test_cmd("tag:mart_helpdesk", threads=2)

    wait_sync = dbt_sync_sensor()

    run_actions = SSHOperator(
        task_id="dbt_run_fsd_actions",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_actions_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    run_enriched = SSHOperator(
        task_id="dbt_run_fsd_enriched",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_enriched_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    run_ol_marts = SSHOperator(
        task_id="dbt_run_fsd_ol_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_ol_marts_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    run_backlog = SSHOperator(
        task_id="dbt_run_fsd_backlog",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_backlog_run') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
        outlets=FSD_MART_OUTLETS,
    )
    test_marts = SSHOperator(
        task_id="dbt_test_fsd_marts",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_marts_test') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )

    actions_cmd = build_actions_run()
    enriched_cmd = build_enriched_run()
    ol_cmd = build_ol_marts_run()
    backlog_cmd = build_backlog_run()
    test_cmd = build_marts_test()
    (
        wait_sync
        >> actions_cmd
        >> run_actions
        >> enriched_cmd
        >> run_enriched
        >> ol_cmd
        >> run_ol_marts
        >> backlog_cmd
        >> run_backlog
        >> test_cmd
        >> test_marts
    )


run_fsd_marts()
