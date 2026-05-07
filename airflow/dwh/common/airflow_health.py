"""Airflow metadata DB health checks with optional Telegram alerts."""

from __future__ import annotations

import json
import logging
import os
import textwrap
from typing import List

import psycopg2
import requests
from airflow.sdk import Variable

logger = logging.getLogger(__name__)

DB_HOST = "s-sd-anl-db00.example.com"
DB_PORT = 5432
DB_NAME = "airflow_db"
DB_USER = "airflow_user"

DAG_ID_PREFIXES = ("ingest_", "dwh_")


def get_variable_list(var_name: str, default: list[str] | None = None) -> list[str] | None:
    try:
        val = Variable.get(var_name, default=None)
    except Exception:
        return default
    if not val:
        return default
    try:
        parsed = json.loads(val)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return [s.strip() for s in val.split(",") if s.strip()]


def send_telegram(text: str) -> None:
    token = Variable.get("telegram_techsup", default=None)
    chat_id = Variable.get("telegram_techsup_chat_id", default=None)
    if not token or not chat_id:
        logger.info("Telegram not configured; would send: %s", text[:200])
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        requests.post(url, json=payload, timeout=10)
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to send Telegram alert: %s", exc)


def _rows_to_text(rows, columns, limit: int = 5) -> str:
    lines = []
    for row in rows[:limit]:
        parts = [f"{col}={val}" for col, val in zip(columns, row)]
        lines.append("; ".join(parts))
    return "\n".join(lines)


def _dag_id_filter_sql() -> tuple[str, list[str]]:
    clauses = " OR ".join(["dag_id LIKE %s" for _ in DAG_ID_PREFIXES])
    return f" AND ({clauses})", [f"{p}%" for p in DAG_ID_PREFIXES]


def check_health_and_alert(
    *,
    task_states: List[str] | None = None,
    dagrun_states: List[str] | None = None,
    lookback_minutes: int = 15,
) -> bool:
    task_states = task_states or get_variable_list(
        "airflow_health_task_states", ["failed", "upstream_failed", "up_for_retry"]
    )
    dagrun_states = dagrun_states or get_variable_list("airflow_health_dagrun_states", ["failed"])

    pwd = Variable.get("airflow_meta_db_password", default=None)
    if not pwd:
        raise RuntimeError("Airflow Variable airflow_meta_db_password is not set")

    interval = f"{int(lookback_minutes)} minutes"
    dag_filter_sql, dag_filter_params = _dag_id_filter_sql()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=pwd
    )
    try:
        cur = conn.cursor()
        parts: list[str] = []

        if task_states:
            placeholders = ",".join(["%s"] * len(task_states))
            sql_ti = textwrap.dedent(
                f"""
                SELECT id, task_id, dag_id, run_id, start_date, end_date, duration, state,
                       try_number, max_tries, hostname, updated_at
                FROM task_instance
                WHERE state IN ({placeholders})
                  AND (updated_at >= now() - %s::interval OR start_date >= now() - %s::interval)
                  {dag_filter_sql}
                ORDER BY updated_at DESC
                LIMIT 200
                """
            )
            params = task_states + [interval, interval] + dag_filter_params
            cur.execute(sql_ti, params)
            ti_rows = cur.fetchall()
            ti_cols = [d[0] for d in cur.description]
            if ti_rows:
                parts.append(f"TaskInstance matches: {len(ti_rows)}")
                parts.append(_rows_to_text(ti_rows, ti_cols))

        if dagrun_states:
            placeholders = ",".join(["%s"] * len(dagrun_states))
            sql_dr = textwrap.dedent(
                f"""
                SELECT id, dag_id, run_id, state, start_date, end_date, queued_at, logical_date, updated_at
                FROM dag_run
                WHERE state IN ({placeholders})
                  AND (updated_at >= now() - %s::interval OR start_date >= now() - %s::interval)
                  {dag_filter_sql}
                ORDER BY updated_at DESC
                LIMIT 200
                """
            )
            params = dagrun_states + [interval, interval] + dag_filter_params
            cur.execute(sql_dr, params)
            dr_rows = cur.fetchall()
            dr_cols = [d[0] for d in cur.description]
            if dr_rows:
                parts.append(f"DagRun matches: {len(dr_rows)}")
                parts.append(_rows_to_text(dr_rows, dr_cols))

        if parts:
            host = os.uname().nodename if hasattr(os, "uname") else os.getenv("COMPUTERNAME", "unknown")
            header = f"DWH Airflow health alert - host={host} lookback={lookback_minutes}m"
            message = header + "\n\n" + "\n\n".join(parts)
            if len(message) > 3900:
                message = message[:3900] + "\n..."
            send_telegram(message)
            return True
        return False
    finally:
        conn.close()
