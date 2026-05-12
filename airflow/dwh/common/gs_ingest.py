"""Google Sheets bronze ingest (full snapshot, no watermark)."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from dwh.common.api_ingest import upload_api_bronze
from dwh.common.gs_client import read_range

logger = logging.getLogger(__name__)

SOURCE = "gs"

_GOOGLE_SHEETS_EPOCH = pd.Timestamp("1899-12-30")


def _normalize_gs_date(value: object) -> str | None:
    """Normalize Google Sheets dates: DD.MM.YYYY / ISO / serial → YYYY-MM-DD[.HH:MM:SS].

    Note: pd.to_datetime(..., dayfirst=True) breaks ISO ``YYYY-MM-DD``
    (``2026-09-02`` → ``2026-02-09``). Parse ISO and DD.MM.YYYY explicitly.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            parsed = _GOOGLE_SHEETS_EPOCH + pd.Timedelta(days=float(value))
            return _format_gs_timestamp(parsed)
        except (ValueError, OverflowError):
            return None
# datetime/date from gspread - take the calendar date of the sheet, not "now"
    if hasattr(value, "strftime") and not isinstance(value, (str, bytes)):
        try:
            parsed = pd.Timestamp(value)
            if pd.isna(parsed):
                return None
            return _format_gs_timestamp(parsed)
        except (ValueError, OverflowError):
            return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return _format_gs_timestamp(value)
    text = str(value).strip()
    if not text:
        return None
    numeric = text.replace(",", ".")
# serial only if the entire string is a number (not an ISO datum with hyphens)
    if numeric.replace(".", "", 1).isdigit():
        try:
            serial = float(numeric)
# Google Sheets dates are usually 20,000..60,000; cut off small debris
            if serial >= 20000:
                parsed = _GOOGLE_SHEETS_EPOCH + pd.Timedelta(days=serial)
                return _format_gs_timestamp(parsed)
        except (ValueError, OverflowError):
            pass
# ISO YYYY-MM-DD[ HH:MM:SS] — without dayfirst (otherwise 2026-09-02 → 2026-02-09)
    parsed = pd.to_datetime(text, format="%Y-%m-%d", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    # EU: DD.MM.YYYY
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, format="%d.%m.%Y", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, format="%d.%m.%Y %H:%M:%S", errors="coerce")
# Miscellaneous (incl. M/D/yyyy with sheet locale) — dayfirst for EU-like
    if pd.isna(parsed):
        parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return text
    return _format_gs_timestamp(parsed)


def _format_gs_timestamp(parsed: pd.Timestamp) -> str:
    if parsed.hour or parsed.minute or parsed.second or parsed.microsecond:
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    return parsed.strftime("%Y-%m-%d")


def _trim_trailing_rows(df: pd.DataFrame, required_column: str | None) -> pd.DataFrame:
    """Drop empty tail rows below the last row with dt + user_name + activity_type."""
    if df.empty:
        return df
    mask = pd.Series(True, index=df.index)
    if required_column and required_column in df.columns:
        mask &= df[required_column].notna() & (df[required_column].astype(str).str.strip() != "")
    if "user_name" in df.columns:
        mask &= df["user_name"].notna() & (df["user_name"].astype(str).str.strip() != "")
    if "activity_type" in df.columns:
        mask &= df["activity_type"].notna() & (df["activity_type"].astype(str).str.strip() != "")
        mask &= ~df["activity_type"].astype(str).str.fullmatch(r"\d+", na=False)
    if not mask.any():
        return df
    last_idx = mask[mask].index[-1]
    trimmed = df.loc[:last_idx].copy()
    if len(trimmed) < len(df):
        logger.info("GS trimmed trailing rows: %s -> %s", len(df), len(trimmed))
    return trimmed


def extract_sheet_entity(
    entity_cfg: dict,
    *,
    conn_id: str,
    spreadsheet_id: str,
) -> pd.DataFrame:
    rows = read_range(
        conn_id=conn_id,
        spreadsheet_id=spreadsheet_id,
        range_name=entity_cfg["range"],
        has_header=bool(entity_cfg.get("has_header")),
        columns=list(entity_cfg["columns"]),
        value_render_option=entity_cfg.get("value_render_option"),
        date_time_render_option=entity_cfg.get("date_time_render_option"),
        short_row_pad=entity_cfg.get(
            "short_row_pad",
            "right" if entity_cfg.get("has_header") else "left",
        ),
        validate_headers=bool(entity_cfg.get("validate_headers")),
    )
    df = pd.DataFrame(rows, columns=list(entity_cfg["columns"]))

    if entity_cfg.get("hours_comma_decimal") and "hours" in df.columns:
        df["hours"] = df["hours"].astype(str).str.replace(",", ".", regex=False)

    for col in entity_cfg.get("comma_decimal_columns") or []:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)

    if entity_cfg.get("blank_to_null"):
        df = df.replace(r"^\s*$", None, regex=True)

    if entity_cfg.get("trim_trailing_rows"):
        df = _trim_trailing_rows(df, entity_cfg.get("required_column"))

    required = entity_cfg.get("required_column")
    if required and required in df.columns:
        df = df[df[required].notna() & (df[required].astype(str).str.strip() != "")]

    for col in entity_cfg.get("required_columns") or []:
        if col in df.columns:
            df = df[df[col].notna() & (df[col].astype(str).str.strip() != "")]

    dedupe_cols = entity_cfg.get("dedupe_columns")
    if dedupe_cols:
        before = len(df)
        df = df.drop_duplicates(subset=list(dedupe_cols), keep="first")
        if before != len(df):
            logger.info(
                "GS dedupe %s: %s -> %s rows (subset=%s)",
                entity_cfg["entity"],
                before,
                len(df),
                dedupe_cols,
            )

    if "activity_type" in df.columns:
        before = len(df)
        df = df[~df["activity_type"].astype(str).str.fullmatch(r"\d+", na=False)]
        if before != len(df):
            logger.info("GS filtered numeric activity_type rows: %s -> %s", before, len(df))

    for col in entity_cfg.get("date_columns") or []:
        if col in df.columns:
            df[col] = df[col].map(_normalize_gs_date)

# Insurance: the dt column is always normalizable (frequent serial bug → "today").
    if "dt" in df.columns and "dt" not in (entity_cfg.get("date_columns") or []):
        df["dt"] = df["dt"].map(_normalize_gs_date)

    date_sample_col = None
    for candidate in ("date", "dt", "activity_date"):
        if candidate in df.columns:
            date_sample_col = candidate
            break
    if date_sample_col:
# Sheets often returns serial (float) + lines in one column —
# pandas Series.min() then TypeError falls on the object.
        sample = df[date_sample_col].dropna().astype(str)
        logger.info(
            "GS %s %s sample: min=%s max=%s distinct=%s",
            entity_cfg["entity"],
            date_sample_col,
            sample.min() if not sample.empty else None,
            sample.max() if not sample.empty else None,
            sample.nunique(),
        )

# Rename to parquet (QA: date; activities: activity_date).
# The column named dt conflicts with hive-partition dt= in S3 path.
    rename_map = entity_cfg.get("bronze_rename_columns") or {}
    if rename_map:
        df = df.rename(columns=dict(rename_map))
        logger.info(
            "GS %s bronze rename %s → columns=%s",
            entity_cfg["entity"],
            rename_map,
            list(df.columns),
        )

    if entity_cfg.get("add_loaded_at"):
        df["loaded_at"] = pd.Timestamp.now(tz="UTC")

    df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    logger.info("GS extract %s: %s rows", entity_cfg["entity"], len(df))
    return df


def run_gs_ingest_task(
    *,
    entity: str,
    entity_cfg: dict,
    conn_id: str,
    spreadsheet_id: str,
    dag_id: str,
    airflow_run_id: str,
    ds: str,
) -> dict:
    legacy = entity_cfg.get("legacy_extract")
    if legacy == "activities" or entity == "esp_techsup_activities":
        from dwh.ingest.gs.extractors.activities import (
            bronze_schema_columns,
            extract_esp_techsup_activities,
        )

        df = extract_esp_techsup_activities(
            entity_cfg, conn_id=conn_id, spreadsheet_id=spreadsheet_id
        )
        parquet_schema_columns = bronze_schema_columns()
    else:
        df = extract_sheet_entity(entity_cfg, conn_id=conn_id, spreadsheet_id=spreadsheet_id)
        parquet_schema_columns = None
    return upload_api_bronze(
        domain=SOURCE,
        entity=entity,
        df=df,
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
        ds=ds,
        watermark_from="snapshot",
        parquet_schema_columns=parquet_schema_columns,
    )
