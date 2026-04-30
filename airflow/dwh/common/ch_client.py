"""ClickHouse HTTP helpers for Airflow workers (audit inserts, light ops)."""

from __future__ import annotations

import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from dwh.common.constants import CH_DATABASES, CH_HOST, CH_PORT

logger = logging.getLogger(__name__)

CH_CONN_ID = "dwh-ch"


def _ch_auth() -> tuple[str, str, str, int]:
    """Resolve host/user/password/port from Airflow conn, Variables, or env."""
    import os

    host = CH_HOST
    port = CH_PORT
    user = "airflow"
    password = ""

    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(CH_CONN_ID)
        if conn.host:
            host = str(conn.host).split("://")[-1].split(":")[0]
        if conn.port:
            port = int(conn.port)
        if conn.login:
            user = conn.login
        if conn.password:
            password = conn.password
    except Exception:
        pass

    try:
        from airflow.models import Variable

        host = Variable.get("dwh_ch_host", default_var=host) or host
        user = Variable.get("dwh_ch_user", default_var=user) or user
        password = Variable.get("dwh_ch_password", default_var=password) or password
        port_raw = Variable.get("dwh_ch_port", default_var=str(port))
        if port_raw:
            port = int(port_raw)
    except Exception:
        pass

    host = os.environ.get("DWH_CH_HOST", host)
    user = os.environ.get("DWH_CH_USER", user)
    password = os.environ.get("DWH_CH_PASSWORD", password)
    port = int(os.environ.get("DWH_CH_PORT", str(port)))
    return host, user, password, port


def ch_execute(sql: str, *, timeout: int = 60, database: str | None = None) -> str:
    host, user, password, port = _ch_auth()
    if not (password or "").strip():
        raise RuntimeError(
            "ClickHouse password is empty. Set Airflow Connection "
            f"`{CH_CONN_ID}` (login/password) or Variables "
            "`dwh_ch_user` / `dwh_ch_password` "
            f"(host default {CH_HOST}:{CH_PORT})."
        )
    params: dict[str, str] = {"user": user, "password": password}
    if database:
        params["database"] = database
    url = f"http://{host}:{port}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:2000]
        except Exception:
            pass
        raise RuntimeError(
            f"ClickHouse HTTP {exc.code} at {host}:{port} user={user!r} "
            f"password_len={len(password)} database={database!r}. "
            f"{body}"
        ) from exc


def _sql_str(value: Any) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_dt(value: Any) -> str:
    if value is None or value == "":
        return "CAST(NULL AS Nullable(DateTime64(3)))"
    return f"parseDateTime64BestEffortOrNull({_sql_str(value)}, 3)"


def insert_audit_bronze_row(manifest: dict, *, manifest_uri: str | None = None) -> None:
    """Append one bronze-run row into sync_*.audit_bronze (no S3 scan)."""
    source = manifest.get("source")
    if not source or source not in CH_DATABASES:
        raise ValueError(f"unknown audit source: {source!r}")

    database = CH_DATABASES[source]
    entity = manifest.get("entity")
    run_id = manifest.get("bronze_run_id") or manifest.get("run_id")
    bronze_dt = manifest.get("bronze_dt")
    row_count = manifest.get("row_count")
    if row_count is None:
        row_count = manifest.get("extracted_row_count")
    watermark_from = manifest.get("watermark_from")
    watermark_to = manifest.get("watermark_to")
    extracted_at = manifest.get("extracted_at")
    dag_id = manifest.get("airflow_dag_id")
    airflow_run_id = manifest.get("airflow_run_id")
    path = manifest_uri or manifest.get("manifest_path")

    sql = (
        f"INSERT INTO `{database}`.`audit_bronze` ("
        "source, entity, bronze_run_id, bronze_dt, row_count, "
        "watermark_from, watermark_to, extracted_at, "
        "airflow_dag_id, airflow_run_id, manifest_path"
        ") VALUES ("
        f"{_sql_str(source)}, {_sql_str(entity)}, {_sql_str(run_id)}, {_sql_str(bronze_dt)}, "
        f"{'NULL' if row_count is None else int(row_count)}, "
        f"{_sql_dt(watermark_from)}, {_sql_dt(watermark_to)}, {_sql_dt(extracted_at)}, "
        f"{_sql_str(dag_id)}, {_sql_str(airflow_run_id)}, {_sql_str(path)}"
        ")"
    )
    ch_execute(sql, database=database)
    logger.info(
        "audit_bronze insert source=%s entity=%s run_id=%s rows=%s",
        source,
        entity,
        run_id,
        row_count,
    )


def try_insert_audit_bronze_row(manifest: dict, *, manifest_uri: str | None = None) -> bool:
    """Best-effort audit insert; never breaks bronze upload on CH errors."""
    try:
        insert_audit_bronze_row(manifest, manifest_uri=manifest_uri)
        return True
    except Exception:
        logger.exception(
            "audit_bronze insert failed source=%s entity=%s run_id=%s",
            manifest.get("source"),
            manifest.get("entity"),
            manifest.get("bronze_run_id") or manifest.get("run_id"),
        )
        return False
