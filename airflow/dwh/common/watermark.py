"""Shared watermark helpers for incremental extract."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

DEFAULT_WATERMARK = "1970-01-01T00:00:00+00:00"
DEFAULT_OVERLAP_MINUTES = 30


def parse_watermark(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def load_watermark(var_name: str, *, default: str = DEFAULT_WATERMARK) -> datetime:
    from airflow.models import Variable

    return parse_watermark(Variable.get(var_name, default_var=default))


def query_from(watermark: datetime, *, overlap_minutes: int = DEFAULT_OVERLAP_MINUTES) -> datetime:
    """Lower bound for incremental extract: stored watermark minus safety overlap."""
    return watermark - timedelta(minutes=overlap_minutes)


def exclusive_start_ts(watermark: datetime, *, overlap_minutes: int = DEFAULT_OVERLAP_MINUTES) -> int:
    """Unix ts for APIs with inclusive 'from' filters (approximate SQL '> watermark')."""
    return int(query_from(watermark, overlap_minutes=overlap_minutes).timestamp()) + 1
