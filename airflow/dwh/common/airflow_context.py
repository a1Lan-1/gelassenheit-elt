"""Airflow task context helpers (MSK ingest hour, metadata DB windows)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
UTC = timezone.utc


def msk_snapshot_hour_str(now: datetime | None = None) -> str:
    """Wall-clock MSK hour start for backlog snapshots (independent of asset/cron interval)."""
    now_msk = (now or datetime.now(MSK)).astimezone(MSK)
    hour = now_msk.replace(minute=0, second=0, microsecond=0)
    return hour.strftime("%Y-%m-%d %H:%M:%S")


def fsd_ingest_check_window_utc(now: datetime | None = None) -> tuple[datetime, datetime]:
    """UTC [start, end) for the current MSK clock hour (ingest wave at :00)."""
    now_msk = (now or datetime.now(MSK)).astimezone(MSK)
    hour_start_msk = now_msk.replace(minute=0, second=0, microsecond=0)
    hour_end_msk = hour_start_msk + timedelta(hours=1)
    return hour_start_msk.astimezone(UTC), hour_end_msk.astimezone(UTC)
