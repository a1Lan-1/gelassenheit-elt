"""Bronze path helpers and manifest builder."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from dwh.common.config import s3_bucket
from dwh.common.constants import CH_DATABASES


def bronze_prefix(source: str, entity: str, dt: str, run_id: str) -> str:
    return f"bronze/{source}/{entity}/dt={dt}/run_id={run_id}"


def bronze_s3_uri(source: str, entity: str, dt: str, run_id: str) -> str:
    prefix = bronze_prefix(source, entity, dt, run_id)
    return f"s3://{s3_bucket()}/{prefix}/data.parquet"


def new_run_id() -> str:
    return str(uuid.uuid4())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_manifest(
    *,
    source: str,
    entity: str,
    run_id: str,
    row_count: int,
    watermark_from: str | None,
    dag_id: str,
    airflow_run_id: str,
    bronze_dt: str | None = None,
    watermark_to: str | None = None,
    watermark_column: str | None = None,
    primary_key: list[str] | None = None,
) -> dict:
    manifest = {
        "source": source,
        "entity": entity,
        "run_id": run_id,
        "bronze_run_id": run_id,
        "target_database": CH_DATABASES[source],
        "extracted_at": utc_now_iso(),
        "watermark_from": watermark_from,
        "watermark_to": watermark_to,
        "row_count": row_count,
        "extracted_row_count": row_count,
        "format": "parquet",
        "schema_version": "v1",
        "airflow_dag_id": dag_id,
        "airflow_run_id": airflow_run_id,
    }
    if bronze_dt is not None:
        manifest["bronze_dt"] = bronze_dt
    if watermark_column is not None:
        manifest["watermark_column"] = watermark_column
    if primary_key is not None:
        manifest["pk"] = primary_key
        manifest["primary_key"] = primary_key
    return manifest


def manifest_json(manifest: dict) -> str:
    return json.dumps(manifest, ensure_ascii=False) + "\n"
