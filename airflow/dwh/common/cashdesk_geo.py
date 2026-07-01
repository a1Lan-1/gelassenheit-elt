"""Offline cashdesk geocoding dictionary for BI / joins.

Source: mv_cashdesk.cashdesk FINAL (address + optional city).
City centers: shared S3 ref (same as ticket geo).
Target: gel_helpdesk.mart_cashdesk_geo (+ _v FINAL view).

Grain: 1 row per cashdesk_id (ReplacingMergeTree by built_at).
Watermark: Airflow Variable dwh_watermark_cashdesk_geo (date_last_update).
"""

from __future__ import annotations

import io
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from dwh.common.ch_client import ch_execute
from dwh.common.constants import CH_DATABASE_HELPDESK
from dwh.common.ticket_geo import (
    DEFAULT_OVERLAP_MINUTES,
    _BAD_CITY_TOKENS,
    build_city_lookup,
    ensure_city_centers_on_s3,
    load_city_centers,
    resolve_city_key,
)

logger = logging.getLogger(__name__)

WATERMARK_VAR = "dwh_watermark_cashdesk_geo"
TARGET_TABLE = "mart_cashdesk_geo"
TARGET_VIEW = "mart_cashdesk_geo_v"
INSERT_BATCH_SIZE = 5000
SOURCE_TABLE = "mv_cashdesk.cashdesk"


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ch_query_df(sql: str, *, timeout: int = 300, database: str | None = None) -> pd.DataFrame:
    text = ch_execute(sql, timeout=timeout, database=database)
    if not text.strip():
        return pd.DataFrame()
    if re.search(r"FORMAT\s+JSONEachRow\b", sql, flags=re.IGNORECASE):
        return pd.read_json(io.StringIO(text), lines=True)
    return pd.read_csv(io.StringIO(text), sep="\t")


def load_watermark() -> str | None:
    from airflow.models import Variable

    raw = (Variable.get(WATERMARK_VAR, default_var="") or "").strip()
    if not raw or raw.startswith("1970-01-01"):
        return None
    return raw


def save_watermark(*, max_updated_at: str, mode: str, rows: int) -> None:
    from airflow.models import Variable

    Variable.set(WATERMARK_VAR, max_updated_at)
    logger.info(
        "watermark saved %s=%s mode=%s rows=%s",
        WATERMARK_VAR,
        max_updated_at,
        mode,
        rows,
    )


def fetch_cashdesks_delta(
    *,
    watermark_updated_at: str | None = None,
    overlap_minutes: int = DEFAULT_OVERLAP_MINUTES,
    full_refresh: bool = False,
) -> tuple[pd.DataFrame, str]:
    """Load cashdesks with non-empty address (full or incremental by date_last_update)."""
    if full_refresh or not watermark_updated_at:
        mode = "backfill"
        where = (
            "c.id > 0 "
            "AND c.address IS NOT NULL "
            "AND trimBoth(c.address) != ''"
        )
    else:
        mode = "incremental"
        wm = pd.Timestamp(watermark_updated_at)
        if wm.tzinfo is None:
            lower = (wm - timedelta(minutes=int(overlap_minutes))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            lower = (wm - timedelta(minutes=int(overlap_minutes))).tz_convert(None).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        where = (
            "c.id > 0 "
            "AND c.address IS NOT NULL "
            "AND trimBoth(c.address) != '' "
            f"AND c.date_last_update >= parseDateTime64BestEffort({_sql_str(lower)}, 3)"
        )

    sql = f"""
SELECT
  toUInt64(c.id) AS cashdesk_id,
  replaceRegexpAll(
    coalesce(nullIf(trimBoth(c.address), ''), ''),
    '[\\\\x00-\\\\x1F]+',
    ' '
  ) AS address,
  coalesce(nullIf(trimBoth(c.city), ''), '') AS city,
  c.date_last_update AS updated_at
FROM {SOURCE_TABLE} AS c FINAL
WHERE {where}
FORMAT JSONEachRow
"""
    # Product DB — do not force gel_helpdesk as default database.
    return _ch_query_df(sql, timeout=600, database=None), mode


def geocode_cashdesks(df: pd.DataFrame, lookup: dict[str, dict[str, Any]]) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    keys: list[str] = []
    resolved: list[str] = []
    lats: list[float | None] = []
    lons: list[float | None] = []
    matched: list[bool] = []

    cities = out.get("city", pd.Series([""] * len(out), index=out.index)).tolist()
    addrs = out.get("address", pd.Series([""] * len(out), index=out.index)).tolist()
    for city, addr in zip(cities, addrs):
        key, hit = resolve_city_key(city, addr, lookup)
        keys.append(key)
        if hit:
            resolved.append(str(hit["name"]))
            lats.append(float(hit["lat"]))
            lons.append(float(hit["lon"]))
            matched.append(True)
        else:
            resolved.append("")
            lats.append(None)
            lons.append(None)
            matched.append(False)

    out["city_norm"] = keys
    out["city_resolved"] = resolved
    out["lat"] = lats
    out["lon"] = lons
    out["geo_matched"] = matched
    out["geo_precision"] = ["city" if ok else "none" for ok in matched]
    out["geo_source"] = ["cashdesk_address" if ok else "" for ok in matched]
    return out


def ensure_target_table() -> None:
    ddl = f"""
CREATE TABLE IF NOT EXISTS {CH_DATABASE_HELPDESK}.{TARGET_TABLE}
(
  cashdesk_id UInt64,
  address String,
  city String,
  city_norm String,
  city_resolved String,
  lat Nullable(Float64),
  lon Nullable(Float64),
  geo_matched Bool,
  geo_precision LowCardinality(String),
  geo_source LowCardinality(String),
  updated_at Nullable(DateTime64(3)),
  built_at DateTime64(3)
)
ENGINE = ReplicatedReplacingMergeTree(built_at)
ORDER BY (cashdesk_id)
"""
    ch_execute(ddl, database=CH_DATABASE_HELPDESK)
    ch_execute(
        f"""
CREATE OR REPLACE VIEW {CH_DATABASE_HELPDESK}.{TARGET_VIEW} AS
SELECT *
FROM {CH_DATABASE_HELPDESK}.{TARGET_TABLE} FINAL
""",
        database=CH_DATABASE_HELPDESK,
    )


def insert_cashdesk_geo(df: pd.DataFrame) -> int:
    ensure_target_table()
    if df.empty:
        return 0

    def _ch_str(value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        text = str(value).replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
        if text.lower() in _BAD_CITY_TOKENS:
            return ""
        return text

    def _ch_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        if num != num or num in (float("inf"), float("-inf")):
            return None
        return num

    def _ch_dt(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        ts = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts.tz_convert(None).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _jsonable(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return int(value)
        if isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):
                return None
            return value
        if isinstance(value, str):
            return value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        item = getattr(value, "item", None)
        if callable(item):
            try:
                return _jsonable(item())
            except Exception:
                pass
        return value

    built = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    n = len(df)
    ids = pd.to_numeric(df["cashdesk_id"], errors="coerce").fillna(0).astype("uint64").tolist()
    addrs = [_ch_str(v) for v in df.get("address", pd.Series([""] * n)).tolist()]
    cities = [_ch_str(v) for v in df.get("city", pd.Series([""] * n)).tolist()]
    norms = [_ch_str(v) for v in df.get("city_norm", pd.Series([""] * n)).tolist()]
    resolved = [_ch_str(v) for v in df.get("city_resolved", pd.Series([""] * n)).tolist()]
    lats = [_ch_float(v) for v in df.get("lat", pd.Series([None] * n)).tolist()]
    lons = [_ch_float(v) for v in df.get("lon", pd.Series([None] * n)).tolist()]
    matched = [
        bool(v) if not (v is None or (isinstance(v, float) and pd.isna(v))) else False
        for v in df.get("geo_matched", pd.Series([False] * n)).tolist()
    ]
    precision = [_ch_str(v) or "none" for v in df.get("geo_precision", pd.Series(["none"] * n)).tolist()]
    source = [_ch_str(v) for v in df.get("geo_source", pd.Series([""] * n)).tolist()]
    updated = [_ch_dt(v) for v in df.get("updated_at", pd.Series([None] * n)).tolist()]

    records: list[dict[str, Any]] = []
    dropped = 0
    for i in range(n):
        if not ids[i]:
            dropped += 1
            continue
        records.append(
            {
                "cashdesk_id": int(ids[i]),
                "address": addrs[i],
                "city": cities[i],
                "city_norm": norms[i],
                "city_resolved": resolved[i],
                "lat": lats[i],
                "lon": lons[i],
                "geo_matched": bool(matched[i]),
                "geo_precision": precision[i],
                "geo_source": source[i] or "cashdesk_address",
                "updated_at": updated[i],
                "built_at": built,
            }
        )
    if dropped:
        logger.warning("dropped %s rows with invalid cashdesk_id", dropped)
    if not records:
        return 0

    inserted = 0
    for i in range(0, len(records), INSERT_BATCH_SIZE):
        chunk = records[i : i + INSERT_BATCH_SIZE]
        payload = "\n".join(
            json.dumps({k: _jsonable(v) for k, v in row.items()}, ensure_ascii=False, allow_nan=False)
            for row in chunk
        )
        sql = f"INSERT INTO {CH_DATABASE_HELPDESK}.{TARGET_TABLE} FORMAT JSONEachRow\n{payload}"
        ch_execute(sql, timeout=300, database=CH_DATABASE_HELPDESK)
        inserted += len(chunk)
    return inserted


def run_cashdesk_geo(
    *,
    force_ref_refresh: bool = False,
    force_full_refresh: bool = False,
    overlap_minutes: int = DEFAULT_OVERLAP_MINUTES,
) -> dict[str, Any]:
    ref = ensure_city_centers_on_s3(force=force_ref_refresh)
    cities = load_city_centers()
    lookup = build_city_lookup(cities)

    wm = None if force_full_refresh else load_watermark()
    watermark_before = wm

    rows, mode = fetch_cashdesks_delta(
        watermark_updated_at=watermark_before,
        overlap_minutes=overlap_minutes,
        full_refresh=force_full_refresh,
    )
    geo = geocode_cashdesks(rows, lookup)
    inserted = insert_cashdesk_geo(geo)

    matched = int(geo["geo_matched"].sum()) if not geo.empty else 0
    watermark_after = watermark_before

    if not geo.empty:
        max_updated = pd.to_datetime(geo["updated_at"], errors="coerce").max()
        if pd.notna(max_updated):
            watermark_after = max_updated.strftime("%Y-%m-%d %H:%M:%S")
            save_watermark(max_updated_at=watermark_after, mode=mode, rows=inserted)
        else:
            # date_last_update can be null — still advance wall-clock so we don't loop forever
            watermark_after = datetime.now(timezone.utc).replace(microsecond=0).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            save_watermark(max_updated_at=watermark_after, mode=mode, rows=inserted)
    elif mode == "backfill":
        watermark_after = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        save_watermark(max_updated_at=watermark_after, mode=mode, rows=0)
    else:
        logger.info("incremental empty; watermark unchanged at %s", watermark_before)

    return {
        "mode": mode,
        "ref_updated": ref["updated"],
        "ref_rows": int(ref["manifest"].get("row_count") or len(cities)),
        "lookup_keys": len(lookup),
        "cashdesks": int(len(geo)),
        "matched": matched,
        "match_rate": (matched / len(geo)) if len(geo) else 0.0,
        "inserted": inserted,
        "watermark_var": WATERMARK_VAR,
        "watermark_before": watermark_before,
        "watermark_after": watermark_after,
        "table": f"{CH_DATABASE_HELPDESK}.{TARGET_TABLE}",
        "view": f"{CH_DATABASE_HELPDESK}.{TARGET_VIEW}",
    }
