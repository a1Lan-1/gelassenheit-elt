"""DWH Airflow health monitor (legacy airflow_health_monitor parity, ingest_/dwh_ DAGs only)."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="dwh_airflow_health_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "ops", "health"],
    max_active_runs=1,
)
def dwh_airflow_health_monitor():
    @task
    def run_health_check() -> bool:
        from dwh.common.airflow_health import check_health_and_alert

        return check_health_and_alert(lookback_minutes=15)

    run_health_check()


dwh_airflow_health_monitor()
