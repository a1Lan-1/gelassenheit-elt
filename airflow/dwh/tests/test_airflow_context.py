from __future__ import annotations

from datetime import datetime, timezone
from dwh.common.airflow_context import (
    fsd_ingest_check_window_utc,
    msk_snapshot_hour_str,
)


def test_msk_snapshot_hour_str_is_hour_start():
    now = datetime(2026, 7, 10, 14, 37, 42, tzinfo=timezone.utc)
    assert msk_snapshot_hour_str(now) == "2026-07-10 17:00:00"


def test_fsd_ingest_check_window_one_hour_utc():
    now = datetime(2026, 7, 10, 14, 37, 42, tzinfo=timezone.utc)
    start, end = fsd_ingest_check_window_utc(now)
    assert start.tzinfo == timezone.utc
    assert end.tzinfo == timezone.utc
    assert start == datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 7, 10, 15, 0, tzinfo=timezone.utc)
    assert (end - start).total_seconds() == 3600
