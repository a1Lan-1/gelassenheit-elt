"""Read-only Airflow metadata queries via PostgreSQL.

Airflow 3.0 task code must not use the ORM (`create_session` / `session.query`).
Use these helpers instead (same pattern as `airflow_health.py`).
"""

from __future__ import annotations

from datetime import datetime

import psycopg2
from airflow.sdk import Variable
from airflow.utils.state import DagRunState, TaskInstanceState

from dwh.common.airflow_health import DB_HOST, DB_NAME, DB_PORT, DB_USER


def _meta_connect():
    pwd = Variable.get("airflow_meta_db_password", default=None)
    if not pwd:
        raise RuntimeError("Airflow Variable airflow_meta_db_password is not set")
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=pwd
    )


def latest_dagrun_state(
    dag_id: str,
    hour_start: datetime,
    hour_end: datetime,
) -> str | None:
    """Latest dag_run.state for *dag_id* whose data_interval_end falls in [hour_start, hour_end)."""
    sql = """
        SELECT state
        FROM dag_run
        WHERE dag_id = %s
          AND data_interval_end >= %s
          AND data_interval_end < %s
        ORDER BY data_interval_end DESC
        LIMIT 1
    """
    conn = _meta_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (dag_id, hour_start, hour_end))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def count_successful_fsd_watermarks(hour_start: datetime, hour_end: datetime) -> int:
    """Count successful FSD ingest watermark tasks finished in [hour_start, hour_end)."""
    sql = """
        SELECT count(*)
        FROM task_instance
        WHERE dag_id LIKE 'ingest_fsd_%%'
          AND task_id = 'watermark'
          AND state = %s
          AND end_date >= %s
          AND end_date < %s
    """
    conn = _meta_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (TaskInstanceState.SUCCESS, hour_start, hour_end),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


def fsd_ingest_dag_ids_missing_success(
    dag_ids: list[str],
    hour_start: datetime,
    hour_end: datetime,
) -> list[str]:
    """Return ingest DAG ids without a successful run in the hour window."""
    missing: list[str] = []
    for dag_id in dag_ids:
        state = latest_dagrun_state(dag_id, hour_start, hour_end)
        if state != DagRunState.SUCCESS:
            missing.append(dag_id)
    return missing
