"""Nightly cleanup of leftover dbt-clickhouse orphan tables in sync_* ClickHouse databases."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.ch_ops import drop_dbt_orphan_tables_script
from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT


@dag(
    dag_id="dwh_ops_cleanup_dbt_orphans",
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "ops", "cleanup"],
    max_active_runs=1,
    doc_md="""
    Drops orphaned dbt-clickhouse tables in `sync_*` databases:

    - `*__dbt_tmp` — failed incremental/swap temp tables
    - `*__dbt_backup` — leftover backup after table swap
    - `*__dbt_new_data*` — e.g. `mart_ticket_actions__dbt_new_data_<uuid>`
    - `__dbt_exchange_test*` — adapter exchange probe tables

    Replaces separate `dwh_ops_cleanup_dbt_tmp` and `dwh_ops_cleanup_dbt_exchange_test` DAGs.
    """,
)
def ops_cleanup_dbt_orphans():
    @task
    def build_cleanup_command() -> str:
        return drop_dbt_orphan_tables_script()

    cleanup = SSHOperator(
        task_id="drop_dbt_orphan_tables",
        ssh_conn_id=SSH_CONN_DBT,
        pool=DBT_SSH_POOL,
        command="{{ ti.xcom_pull(task_ids='build_cleanup_command') }}",
        cmd_timeout=SSH_DBT_CMD_TIMEOUT,
    )
    build_cleanup_command() >> cleanup


ops_cleanup_dbt_orphans()
