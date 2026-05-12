"""Nextcloud Calltraffic → bronze/gs ingest (full snapshot)."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from dwh.common.api_ingest import upload_api_bronze
from dwh.common.nc_client import download_dav_file
from dwh.common.nc_xlsx_schedule import flatten_schedule_workbook, parse_employees_sheet

logger = logging.getLogger(__name__)

SOURCE = "gs"


def run_nc_ingest_task(
    *,
    entity: str,
    entity_cfg: dict[str, Any],
    root_cfg: dict[str, Any],
    dag_id: str,
    airflow_run_id: str,
    ds: str,
) -> dict:
    conn_id = root_cfg.get("conn_id", "nextcloud_calltrafic")
    host = root_cfg.get("host", "cloud.calltraffic.ru")
    dav_path = root_cfg["dav_path"]
    employees_sheet = root_cfg.get("employees_sheet", "Employee list")

    data = download_dav_file(conn_id=conn_id, host=host, dav_path=dav_path)
    kind = entity_cfg.get("kind")

    if kind == "schedule":
        df = flatten_schedule_workbook(
            data,
            employees_sheet=employees_sheet,
            plan_col_1based=int(entity_cfg.get("plan_col_1based", 27)),
            name_col_1based=int(entity_cfg.get("name_col_1based", 2)),
            date_col_1based=int(entity_cfg.get("date_col_1based", 30)),
            skip_name_labels=list(entity_cfg.get("skip_name_labels") or []),
        )
    elif kind == "employees":
        df = parse_employees_sheet(
            data,
            sheet=str(entity_cfg.get("sheet") or employees_sheet),
            header_map=dict(entity_cfg.get("header_map") or {}),
            columns=list(entity_cfg["columns"]),
            required_column=entity_cfg.get("required_column", "full_name"),
        )
    else:
        raise ValueError(f"Unknown nc entity kind: {kind!r} for {entity}")

    df["ingested_at"] = pd.Timestamp.now(tz="UTC")
    logger.info("NC extract %s: %s rows", entity, len(df))

    return upload_api_bronze(
        domain=SOURCE,
        entity=entity,
        df=df,
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
        ds=ds,
        watermark_from="snapshot",
    )
