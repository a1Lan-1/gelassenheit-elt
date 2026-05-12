"""Shared FSD bronze ingest helpers."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dwh.common.bronze import build_manifest, manifest_json, new_run_id
from dwh.common.constants import SOURCE_CONN_FSD
from dwh.common.s3 import upload_file_to_s3
from dwh.common.timezone_utils import ensure_msk_aware, format_msk_watermark, to_msk_naive_iso
from dwh.common.watermark import DEFAULT_WATERMARK, parse_watermark, query_from

logger = logging.getLogger(__name__)

SOURCE = "helpdesk"
DEFAULT_BATCH_SIZE = 100_000


def watermark_var(entity: str) -> str:
    return f"dwh_watermark_fsd_{entity}"


def dbt_current_model(entity: str) -> str:
    return f"int_helpdesk__{entity}_current"


def _serialize_value(val: Any) -> Any:
    if isinstance(val, uuid.UUID):
        return str(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    if isinstance(val, (np.integer, np.floating)):
        return float(val) if isinstance(val, np.floating) else int(val)
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, (datetime, pd.Timestamp)):
        if pd.isna(val):
            return None
        return to_msk_naive_iso(val)
    return val


def _json_cell_to_string(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def _json_columns(entity_cfg: dict) -> set[str]:
    names = set(entity_cfg.get("json_columns") or [])
    for col in entity_cfg.get("columns") or []:
        if col.get("semantic_type") == "json":
            names.add(col["name"])
    return names


def batch_size(entity_cfg: dict) -> int:
    return int(entity_cfg.get("batch_size") or DEFAULT_BATCH_SIZE)


def _order_by_clause(entity_cfg: dict, watermark_column: str) -> str:
    pk = entity_cfg.get("primary_key") or ["id"]
    columns = [watermark_column]
    for column in pk:
        if column not in columns:
            columns.append(column)
    return ", ".join(columns)


def _watermark_predicate(
    entity_cfg: dict,
    *,
    watermark_column: str,
    cursor_after: dict | None,
) -> str:
    pk_col = (entity_cfg.get("primary_key") or ["id"])[0]
    if cursor_after is None:
        return f"{watermark_column} > %(watermark)s"
    return (
        f"({watermark_column} > %(cursor_wm)s OR "
        f"({watermark_column} = %(cursor_wm)s AND {pk_col} > %(cursor_pk)s))"
    )


def _build_extract_sql(
    entity_cfg: dict,
    *,
    watermark_column: str,
    batch_limit: int,
    cursor_after: dict | None = None,
) -> str:
    predicate = _watermark_predicate(
        entity_cfg,
        watermark_column=watermark_column,
        cursor_after=cursor_after,
    )
    extract_sql = entity_cfg.get("extract_sql")
    if extract_sql:
        sql = extract_sql.rstrip().rstrip(";")
        if cursor_after is not None:
            sql = sql.replace(f"{watermark_column} > %(watermark)s", predicate)
    else:
        source_table = entity_cfg["source_table"]
        order_by = _order_by_clause(entity_cfg, watermark_column)
        sql = (
            f"SELECT * FROM {source_table} "
            f"WHERE {predicate} "
            f"ORDER BY {order_by}"
        )
    return f"{sql}\nLIMIT %(limit)s"


def _batch_cursor(df: pd.DataFrame, entity_cfg: dict, watermark_column: str) -> dict | None:
    if df.empty:
        return None
    pk_col = (entity_cfg.get("primary_key") or ["id"])[0]
    last = df.iloc[-1]
    watermark_value = last[watermark_column]
    if pd.isna(watermark_value):
        return None
    return {
        "watermark_value": format_msk_watermark(watermark_value),
        "pk": _serialize_value(last[pk_col]),
    }


def _query_parameters(
    *,
    watermark_query_at: datetime,
    batch_limit: int,
    cursor_after: dict | None,
) -> dict[str, Any]:
    if cursor_after is None:
        return {
            "watermark": ensure_msk_aware(watermark_query_at),
            "limit": batch_limit,
        }
    return {
        "cursor_wm": ensure_msk_aware(parse_watermark(cursor_after["watermark_value"])),
        "cursor_pk": cursor_after["pk"],
        "limit": batch_limit,
    }


def _prepare_dataframe(df: pd.DataFrame, entity_cfg: dict) -> pd.DataFrame:
    for column in df.columns:
        df[column] = df[column].apply(_serialize_value)

    for column in _json_columns(entity_cfg):
        if column in df.columns:
            df[column] = df[column].apply(_json_cell_to_string)

    for column, default in (entity_cfg.get("fillna") or {}).items():
        if column in df.columns:
            df[column] = df[column].fillna(default)

    return df


def extract_entity(
    entity_cfg: dict,
    *,
    dag_id: str,
    airflow_run_id: str,
    ds: str,
    watermark_raw: str | None = None,
    apply_overlap: bool = True,
    batch_limit: int | None = None,
    cursor_after: dict | None = None,
) -> dict:
    from airflow.models import Variable

    entity = entity_cfg["entity"]
    var_name = watermark_var(entity)
    if watermark_raw is None:
        watermark_raw = Variable.get(var_name, default_var=DEFAULT_WATERMARK)
    watermark_at = parse_watermark(watermark_raw)
    if cursor_after is not None:
        watermark_query_at = ensure_msk_aware(watermark_at)
        apply_overlap = False
    else:
        watermark_query_at = (
            query_from(watermark_at) if apply_overlap else ensure_msk_aware(watermark_at)
        )
    watermark_column = entity_cfg.get("watermark_column", "updated_at")
    limit = batch_limit if batch_limit is not None else batch_size(entity_cfg)

    hook = PostgresHook(postgres_conn_id=SOURCE_CONN_FSD)
    sql = _build_extract_sql(
        entity_cfg,
        watermark_column=watermark_column,
        batch_limit=limit,
        cursor_after=cursor_after,
    )

    df = hook.get_pandas_df(
        sql,
        parameters=_query_parameters(
            watermark_query_at=watermark_query_at,
            batch_limit=limit,
            cursor_after=cursor_after,
        ),
    )
    batch_cursor = _batch_cursor(df, entity_cfg, watermark_column)
    df = _prepare_dataframe(df, entity_cfg)
    watermark_to = None
    if not df.empty and watermark_column in df.columns:
        max_watermark = pd.to_datetime(df[watermark_column], errors="coerce").max()
        if pd.notna(max_watermark):
            watermark_to = format_msk_watermark(max_watermark)

    run_id = new_run_id()
    tmp_dir = tempfile.mkdtemp(prefix="dwh_")
    local_parquet = os.path.join(tmp_dir, "data.parquet")
    local_manifest = os.path.join(tmp_dir, "manifest.json")

    df.to_parquet(local_parquet, index=False)
    pk = entity_cfg.get("primary_key", ["id"])
    manifest = build_manifest(
        source=SOURCE,
        entity=entity,
        run_id=run_id,
        row_count=len(df),
        watermark_from=watermark_raw,
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
        bronze_dt=ds,
        watermark_to=watermark_to,
        watermark_column=watermark_column,
        primary_key=pk,
    )
    manifest["watermark_query_from"] = watermark_query_at.isoformat()
    manifest["load_mode"] = entity_cfg.get("load_mode", "upsert_current")
    with open(local_manifest, "w", encoding="utf-8") as handle:
        handle.write(manifest_json(manifest))

    logger.info(
        "Extracted %s rows for entity=%s run_id=%s batch_limit=%s",
        len(df),
        entity,
        run_id,
        limit,
    )
    return {
        "entity": entity,
        "run_id": run_id,
        "dt": ds,
        "local_parquet": local_parquet,
        "local_manifest": local_manifest,
        "manifest": manifest,
        "row_count": len(df),
        "batch_limit": limit,
        "_batch_cursor": batch_cursor,
        "watermark_from": watermark_raw,
        "watermark_query_from": watermark_query_at.isoformat(),
        "watermark_to": watermark_to,
        "watermark_column": watermark_column,
        "source_table": entity_cfg.get("source_table", ""),
    }


def extract_and_upload_batches(
    entity_cfg: dict,
    *,
    dag_id: str,
    airflow_run_id: str,
    ds: str,
) -> list[dict]:
    """Extract/upload ≤batch_size rows per iteration until source is caught up."""
    from airflow.models import Variable

    entity = entity_cfg["entity"]
    var_name = watermark_var(entity)
    watermark_raw = Variable.get(var_name, default_var=DEFAULT_WATERMARK)
    limit = batch_size(entity_cfg)
    batches: list[dict] = []
    cursor_after: dict | None = None

    while True:
        meta = extract_entity(
            entity_cfg,
            dag_id=dag_id,
            airflow_run_id=airflow_run_id,
            ds=ds,
            watermark_raw=watermark_raw,
            apply_overlap=len(batches) == 0,
            batch_limit=limit,
            cursor_after=cursor_after,
        )
        meta = upload_bronze(meta)
        row_count = int(meta.get("row_count", 0))
        cursor_after = meta.pop("_batch_cursor", None)
        if row_count == 0:
            break

        batches.append(meta)
        if row_count < limit:
            break

    total_rows = sum(int(batch.get("row_count", 0)) for batch in batches)
    logger.info(
        "Batched ingest entity=%s batches=%s total_rows=%s",
        entity,
        len(batches),
        total_rows,
    )
    return batches


def run_dbt_for_batches(batches: list[dict], *, model: str) -> None:
    from airflow.exceptions import AirflowException

    from dwh.common.constants import SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT
    from dwh.common.ssh import dbt_run_cmd, dbt_test_cmd, exec_ssh

    for index, meta in enumerate(batches, start=1):
        run_cmd = dbt_run_cmd(
            model,
            bronze_dt=meta["dt"],
            bronze_run_id=meta["run_id"],
            with_current_view=True,
        )
        test_cmd = dbt_test_cmd(model, with_current_view=True)
        code, out, err = exec_ssh(SSH_CONN_DBT, run_cmd, timeout=SSH_DBT_CMD_TIMEOUT)
        if code != 0:
            raise AirflowException(
                f"dbt run failed for {model} batch {index}/{len(batches)}: {err or out}"
            )
        code, out, err = exec_ssh(SSH_CONN_DBT, test_cmd, timeout=SSH_DBT_CMD_TIMEOUT)
        if code != 0:
            raise AirflowException(
                f"dbt test failed for {model} batch {index}/{len(batches)}: {err or out}"
            )
        logger.info(
            "dbt OK for entity=%s batch %s/%s run_id=%s rows=%s",
            meta.get("entity"),
            index,
            len(batches),
            meta.get("run_id"),
            meta.get("row_count"),
        )


def upload_bronze(meta: dict) -> dict:
    from dwh.common.bronze import bronze_s3_uri
    from dwh.common.ch_client import try_insert_audit_bronze_row

    entity = meta["entity"]
    data_uri = bronze_s3_uri(SOURCE, entity, meta["dt"], meta["run_id"])
    manifest_uri = data_uri.replace("data.parquet", "manifest.json")

    if meta["row_count"] == 0:
        logger.info("No rows for entity=%s — uploading manifest only for audit", entity)
        upload_file_to_s3(meta["local_manifest"], manifest_uri)
    else:
        upload_file_to_s3(meta["local_parquet"], data_uri)
        upload_file_to_s3(meta["local_manifest"], manifest_uri)

    # Live audit row — primary path (hourly S3-scan DAG is backfill-only).
    manifest = meta.get("manifest")
    if not manifest and meta.get("local_manifest") and os.path.exists(meta["local_manifest"]):
        with open(meta["local_manifest"], encoding="utf-8") as handle:
            manifest = json.load(handle)
    if manifest:
        try_insert_audit_bronze_row(manifest, manifest_uri=manifest_uri)

    os.remove(meta["local_parquet"])
    os.remove(meta["local_manifest"])
    meta["bronze_uri"] = data_uri
    meta["manifest_uri"] = manifest_uri
    return meta


def update_watermark(meta: dict) -> dict:
    from airflow.models import Variable

    if meta["row_count"] == 0:
        return meta

    entity = meta["entity"]
    if meta.get("watermark_to"):
        Variable.set(watermark_var(entity), meta["watermark_to"])
    return meta
