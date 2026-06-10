"""Dialer OAuth token refresh DAG."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="dwh_refresh_dialer_token",
    schedule="@hourly",
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "dialer", "token"],
)
def dwh_refresh_dialer_token():
    @task
    def refresh() -> str:
        from dwh.common.dialer_token import refresh_dialer_tokens

        return refresh_dialer_tokens()

    refresh()


dwh_refresh_dialer_token()
