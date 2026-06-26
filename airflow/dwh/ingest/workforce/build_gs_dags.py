"""Factory: Google Sheets ingest DAGs → bronze → dbt int_workforce__*_current (FSD pattern)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

from dwh.common.constants import (
    API_EXPORT_MAX_RETRY_DELAY_MIN,
    API_EXPORT_RETRIES,
    API_EXPORT_RETRY_DELAY_MIN,
    DBT_SSH_POOL,
    GS_CONN_ID,
    SSH_CONN_DBT,
    SSH_DBT_CMD_TIMEOUT,
)
from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.assets import gs_cur_view_asset
from dwh.common.schedules import gs_entity_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "gs_entities.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_gs_entity_cfg(entity: str) -> dict:
    """Read entity config at task runtime (DAG parse may cache stale yaml)."""
    cfg = _load_config()
    for item in cfg.get("entities", []):
        if item.get("entity") == entity:
            return item
    raise ValueError(f"entity {entity!r} not found in {CONFIG_PATH}")


def _dbt_current_model(entity: str) -> str:
    return f"int_workforce__{entity}_current"


def _build_dag(entity_cfg: dict, *, conn_id: str, spreadsheet_id: str):
    entity = entity_cfg["entity"]
    model = _dbt_current_model(entity)
    dag_id = f"ingest_gs_{entity}"
    schedule = gs_entity_schedule(entity_cfg.get("schedule"))
    cur_asset = gs_cur_view_asset(entity)

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=datetime(2026, 6, 3),
        catchup=False,
        tags=["dwh", "gs", "ingest", entity],
        max_active_runs=1,
    )
    def _ingest():
        @task(
            retries=API_EXPORT_RETRIES,
            retry_delay=timedelta(minutes=API_EXPORT_RETRY_DELAY_MIN),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=API_EXPORT_MAX_RETRY_DELAY_MIN),
        )
        def extract_and_upload(**context) -> dict:
            from dwh.common.gs_ingest import run_gs_ingest_task

            cfg = _load_config()
            return run_gs_ingest_task(
                entity=entity,
                entity_cfg=get_gs_entity_cfg(entity),
                conn_id=cfg.get("conn_id", conn_id),
                spreadsheet_id=cfg["spreadsheet_id"],
                dag_id=context["dag"].dag_id,
                airflow_run_id=context["run_id"],
                ds=context["ds"],
            )

        @task.short_circuit
        def has_bronze_rows(meta: dict) -> bool:
            return int(meta.get("row_count", 0)) > 0

        @task
        def build_dbt_run_command(meta: dict) -> str:
            from dwh.common.ssh import dbt_run_cmd

            return dbt_run_cmd(
                model,
                bronze_dt=meta["dt"],
                bronze_run_id=meta["run_id"],
                with_current_view=True,
                full_refresh=True,
            )

        @task
        def build_dbt_test_command() -> str:
            from dwh.common.ssh import dbt_test_cmd

            return dbt_test_cmd(model, with_current_view=True)

        wait_sync = dbt_sync_sensor()

        run_dbt = SSHOperator(
            task_id=f"dbt_run_{model}",
            ssh_conn_id=SSH_CONN_DBT,
            pool=DBT_SSH_POOL,
            command="{{ ti.xcom_pull(task_ids='build_dbt_run_command') }}",
            cmd_timeout=SSH_DBT_CMD_TIMEOUT,
        )
        test_dbt = SSHOperator(
            task_id=f"dbt_test_{model}",
            ssh_conn_id=SSH_CONN_DBT,
            pool=DBT_SSH_POOL,
            command="{{ ti.xcom_pull(task_ids='build_dbt_test_command') }}",
            cmd_timeout=SSH_DBT_CMD_TIMEOUT,
            outlets=[cur_asset],
        )

        meta = extract_and_upload()
        gate = has_bronze_rows(meta)
        dbt_run_command = build_dbt_run_command(meta)
        dbt_test_command = build_dbt_test_command()

        meta >> wait_sync >> gate
        gate >> dbt_run_command >> run_dbt >> dbt_test_command >> test_dbt

    return _ingest()


_cfg = _load_config()
_conn_id = _cfg.get("conn_id", GS_CONN_ID)
_spreadsheet_id = _cfg["spreadsheet_id"]

for _entity_item in _cfg.get("entities", []):
    _dag = _build_dag(_entity_item, conn_id=_conn_id, spreadsheet_id=_spreadsheet_id)
    globals()[_dag.dag_id] = _dag
