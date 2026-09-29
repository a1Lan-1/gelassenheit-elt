"""FSD source uses Postgres timestamptz columns that store Moscow wall clock."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

# Postgres timestamptz is read as UTC-aware by psycopg2; business meaning is MSK wall clock.
MSK = timezone(timedelta(hours=3))


def to_datetime(val: datetime | pd.Timestamp) -> datetime:
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    return val


def ensure_msk_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK)


def to_msk_naive(dt: datetime | pd.Timestamp) -> datetime:
    """Convert timestamptz/naive source value to naive Moscow wall clock."""
    value = to_datetime(dt)
    if value.tzinfo is None:
        return value
    return value.astimezone(MSK).replace(tzinfo=None)


def to_msk_naive_iso(val: datetime | pd.Timestamp | None) -> str | None:
    """Serialize datetime for bronze parquet (dbt reads String -> DateTime64 MSK)."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or pd.isna(val):
        return None
    naive = to_msk_naive(val)
    ms = naive.microsecond // 1000
    if ms:
        return naive.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"
    return naive.strftime("%Y-%m-%d %H:%M:%S")


def format_msk_watermark(val: datetime | pd.Timestamp) -> str:
    """Persist watermark Variable as MSK-aware ISO for timestamptz comparisons."""
    aware = ensure_msk_aware(to_datetime(val))
    return aware.replace(microsecond=0).isoformat()
