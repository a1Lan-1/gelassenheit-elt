"""esp_techsup_activities — legacy parity with second_line_activities.py."""

from __future__ import annotations

import logging

import pandas as pd

from dwh.common.gs_client import read_range

logger = logging.getLogger(__name__)

INGEST_CODE_VERSION = "activities-2026-06-30e"
_SHEET_COLUMNS = ["dt", "user_name", "activity_type", "count_hours"]
_BRONZE_COLUMNS = ["activity_date", "user_name", "activity_type", "count_hours", "ingested_at"]
_DEFAULT_RANGE = "schedule_2l_result!A2:D"
_GOOGLE_SHEETS_EPOCH = pd.Timestamp("1899-12-30")


def bronze_schema_columns() -> list[str]:
    return list(_BRONZE_COLUMNS)


def _serial_to_date_str(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            parsed = _GOOGLE_SHEETS_EPOCH + pd.Timedelta(days=float(value))
            return parsed.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return None
    text = str(value).strip()
    if not text:
        return None
    numeric = text.replace(",", ".")
    if numeric.replace(".", "", 1).isdigit():
        try:
            parsed = _GOOGLE_SHEETS_EPOCH + pd.Timedelta(days=float(numeric))
            return parsed.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            pass
    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return text
    return parsed.strftime("%Y-%m-%d")


def _rows_to_frame(rows: list[list[object]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=_SHEET_COLUMNS)
    df = df.replace(r"^\s*$", None, regex=True)
    for col in _SHEET_COLUMNS:
        df[col] = df[col].map(
            lambda v: None
            if v is None or (isinstance(v, float) and pd.isna(v))
            else str(v).strip() or None
        )
    return df


def extract_esp_techsup_activities(
    entity_cfg: dict,
    *,
    conn_id: str,
    spreadsheet_id: str,
) -> pd.DataFrame:
    """Full snapshot from schedule_2l_result!A2:D, same steps as legacy Postgres DAG."""
    range_name = entity_cfg.get("range", _DEFAULT_RANGE)
    common_kwargs = {
        "conn_id": conn_id,
        "spreadsheet_id": spreadsheet_id,
        "range_name": range_name,
        "has_header": False,
        "columns": _SHEET_COLUMNS,
        "date_time_render_option": "FORMATTED_STRING",
    }

    rows = read_range(**common_kwargs)
    df = _rows_to_frame(rows)

    if df["dt"].notna().sum() == 0:
        logger.warning(
            "GS esp_techsup_activities: dt empty with FORMATTED_STRING, retry UNFORMATTED_VALUE"
        )
        rows = read_range(**common_kwargs, value_render_option="UNFORMATTED_VALUE")
        df = _rows_to_frame(rows)
        df["dt"] = df["dt"].map(_serial_to_date_str)

    if df.empty:
        raise ValueError("activities extract returned 0 rows")
    if df["dt"].notna().sum() == 0:
        sample = rows[:3]
        raise ValueError(
            f"activities extract: sheet date column is all null for {range_name}; sample_rows={sample!r}"
        )

    df = df.rename(columns={"dt": "activity_date"})
    sample = df["activity_date"].dropna().head(5).tolist()
    logger.info(
        "GS esp_techsup_activities %s: rows=%s activity_date_distinct=%s sample=%s cols=%s",
        INGEST_CODE_VERSION,
        len(df),
        df["activity_date"].dropna().nunique(),
        sample,
        list(df.columns),
    )

    df["ingested_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    return df[_BRONZE_COLUMNS]
