"""FSD hourly gate: emit dwh://fsd/hourly_ready when all ingest_fsd_* succeeded this hour."""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sdk import dag, task

from dwh.common.airflow_context import fsd_ingest_check_window_utc
from dwh.common.airflow_meta import (
    count_successful_fsd_watermarks,
    fsd_ingest_dag_ids_missing_success,
)
from dwh.common.assets import FSD_HOURLY_READY, fsd_ingest_dag_ids
from dwh.common.schedules import FSD_HOURLY_READY_CRON_MSK, resolve_schedule

logger = logging.getLogger(__name__)


@dag(
    dag_id="dwh_fsd_hourly_ready",
    schedule=resolve_schedule(FSD_HOURLY_READY_CRON_MSK),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "helpdesk", "assets", "coordinator"],
    max_active_runs=1,
    doc_md="""
    At :45 MSK checks that every `ingest_fsd_*` DAG completed successfully for the
    current MSK hour, then emits asset `dwh://fsd/hourly_ready` for `dwh_run_fsd_marts`.
    """,
)
def fsd_hourly_ready():
    @task(outlets=[FSD_HOURLY_READY])
    def verify_fsd_ingests_and_emit() -> None:
        hour_start, hour_end = fsd_ingest_check_window_utc()
        dag_ids = fsd_ingest_dag_ids()
        missing = fsd_ingest_dag_ids_missing_success(dag_ids, hour_start, hour_end)
        if missing:
            raise AirflowException(
                f"FSD hourly gate: {len(missing)}/{len(dag_ids)} ingest DAG(s) not successful "
                f"for MSK hour starting {hour_start.isoformat()}: "
                f"{', '.join(missing[:8])}"
                + ("..." if len(missing) > 8 else "")
            )

        ingested = count_successful_fsd_watermarks(hour_start, hour_end)
        logger.info(
            "FSD hourly gate OK: window=%s..%s watermarks=%s",
            hour_start.isoformat(),
            hour_end.isoformat(),
            ingested,
        )
        if ingested == 0:
            raise AirflowSkipException(
                "FSD hourly gate: no ingest watermark with data this MSK hour; "
                "skipping hourly_ready asset (marts still run on :54 cron)"
            )

    verify_fsd_ingests_and_emit()


fsd_hourly_ready()
