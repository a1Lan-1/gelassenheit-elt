"""Wait for dwh_sync_dbt_repo before running dbt on the dbt host."""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

SYNC_DAG_ID = "dwh_sync_dbt_repo"
SYNC_TASK_ID = "git_sync_dbt_etl"


def sync_dbt_execution_date(logical_date: datetime, **kwargs) -> datetime:
    """Align worker DAG run time with */5 sync DAG schedule.

    For cron DAGs, logical_date may represent interval start (can be previous day).
    Prefer data_interval_end so the sensor targets the sync run around actual start.
    """
    base_dt = kwargs.get("data_interval_end") or logical_date
    return base_dt.replace(
        minute=(base_dt.minute // 5) * 5,
        second=0,
        microsecond=0,
    )


def dbt_sync_sensor() -> ExternalTaskSensor:
    return ExternalTaskSensor(
        task_id="wait_dbt_repo_sync",
        external_dag_id=SYNC_DAG_ID,
        external_task_id=SYNC_TASK_ID,
        execution_date_fn=sync_dbt_execution_date,
        mode="reschedule",
        poke_interval=60,
        timeout=900,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        check_existence=True,
    )
