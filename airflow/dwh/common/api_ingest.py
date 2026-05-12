"""Shared API bronze ingest helpers (CRM, PBX, Dialer)."""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime
from typing import Callable

import pandas as pd

from dwh.common.bronze import build_manifest, manifest_json, new_run_id
from dwh.common.s3 import upload_file_to_s3
from dwh.common.watermark import DEFAULT_WATERMARK, load_watermark

logger = logging.getLogger(__name__)


def watermark_var(domain: str, entity: str) -> str:
    return f"dwh_watermark_{domain}_{entity}"


def load_api_watermark(domain: str, entity: str) -> datetime:
    """First run starts from epoch; incremental runs use Airflow Variable."""
    return load_watermark(watermark_var(domain, entity))


def _write_parquet_with_schema(
    df: pd.DataFrame, path: str, columns: list[str], *, label: str = ""
) -> None:
    """Write parquet keeping all columns (pyarrow may drop all-null object columns)."""
    length = len(df)
    out: dict[str, pd.Series] = {}
    for col in columns:
        if col in df.columns and length:
            series = df[col].map(
                lambda v: None
                if v is None or (isinstance(v, float) and pd.isna(v))
                else str(v).strip() or None
            )
        else:
            series = pd.Series([None] * length, dtype="object")
        out[col] = series.astype("string")
    frame = pd.DataFrame(out)[columns] if columns else pd.DataFrame(out)
    frame.to_parquet(path, index=False)
    try:
        import pyarrow.parquet as pq

        names = pq.read_schema(path).names
        logger.info(
            "bronze parquet %s columns=%s rows=%s",
            label or path,
            names,
            len(frame),
        )
        missing = [c for c in columns if c not in names]
        if missing:
            raise ValueError(f"bronze parquet missing columns {missing}; got {names}")
    except Exception as exc:
        logger.warning("bronze parquet schema check failed: %s", exc)


def upload_api_bronze(
    *,
    domain: str,
    entity: str,
    df: pd.DataFrame,
    dag_id: str,
    airflow_run_id: str,
    ds: str,
    watermark_from: str,
    parquet_schema_columns: list[str] | None = None,
) -> dict:
    run_id = new_run_id()
    tmp_dir = tempfile.mkdtemp(prefix="dwh_api_")
    local_parquet = os.path.join(tmp_dir, "data.parquet")
    local_manifest = os.path.join(tmp_dir, "manifest.json")

    if parquet_schema_columns:
        _write_parquet_with_schema(
            df,
            local_parquet,
            parquet_schema_columns,
            label=f"{domain}.{entity}",
        )
    else:
        df.to_parquet(local_parquet, index=False)
    watermark_to = None
    if getattr(df, "attrs", None) and df.attrs.get("watermark_to"):
        watermark_to = df.attrs["watermark_to"]
    elif "updated_at" in df.columns and not df.empty:
        max_ts = pd.to_datetime(df["updated_at"], utc=True, errors="coerce").max()
        if pd.notna(max_ts):
            watermark_to = max_ts.isoformat()
    elif "created_at" in df.columns and not df.empty:
        max_ts = pd.to_datetime(df["created_at"], utc=True, errors="coerce").max()
        if pd.notna(max_ts):
            watermark_to = max_ts.isoformat()
    elif "started_at" in df.columns and not df.empty:
        max_ts = pd.to_datetime(df["started_at"], utc=True, errors="coerce").max()
        if pd.notna(max_ts):
            watermark_to = max_ts.isoformat()
    elif "date_h" in df.columns and not df.empty:
        max_ts = pd.to_datetime(df["date_h"], utc=True, errors="coerce").max()
        if pd.notna(max_ts):
            watermark_to = max_ts.isoformat()
    manifest = build_manifest(
        source=domain,
        entity=entity,
        run_id=run_id,
        row_count=len(df),
        watermark_from=watermark_from,
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
        bronze_dt=ds,
        watermark_to=watermark_to,
    )
    with open(local_manifest, "w", encoding="utf-8") as handle:
        handle.write(manifest_json(manifest))

    meta = {
        "domain": domain,
        "entity": entity,
        "run_id": run_id,
        "dt": ds,
        "local_parquet": local_parquet,
        "local_manifest": local_manifest,
        "row_count": len(df),
        "watermark_from": watermark_from,
        "watermark_to": watermark_to,
    }

    from dwh.common.bronze import bronze_s3_uri
    from dwh.common.ch_client import try_insert_audit_bronze_row

    data_uri = bronze_s3_uri(domain, entity, ds, run_id)
    manifest_uri = data_uri.replace("data.parquet", "manifest.json")

    if meta["row_count"] == 0:
        logger.info("No API rows for %s.%s — uploading manifest only for audit", domain, entity)
        upload_file_to_s3(local_manifest, manifest_uri)
    else:
        upload_file_to_s3(local_parquet, data_uri)
        upload_file_to_s3(local_manifest, manifest_uri)

    # Live audit row — primary path (hourly S3-scan DAG is backfill-only).
    try_insert_audit_bronze_row(manifest, manifest_uri=manifest_uri)

    os.remove(local_parquet)
    os.remove(local_manifest)
    meta["bronze_uri"] = data_uri
    meta["manifest_uri"] = manifest_uri
    return meta


def run_api_ingest_task(
    *,
    domain: str,
    entity: str,
    extract_fn: Callable[[], pd.DataFrame],
    dag_id: str,
    airflow_run_id: str,
    ds: str,
) -> dict:
    from airflow.models import Variable

    var_name = watermark_var(domain, entity)
    watermark_from = Variable.get(var_name, default_var=DEFAULT_WATERMARK)
    df = extract_fn()
    meta = upload_api_bronze(
        domain=domain,
        entity=entity,
        df=df,
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
        ds=ds,
        watermark_from=watermark_from,
    )
    return meta


def update_api_watermark(meta: dict) -> dict:
    from airflow.models import Variable

    if meta.get("watermark_to"):
        Variable.set(watermark_var(meta["domain"], meta["entity"]), meta["watermark_to"])
    return meta
