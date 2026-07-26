"""Tests for FSD timestamptz-as-MSK normalization."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dwh.common.timezone_utils import MSK, ensure_msk_aware, format_msk_watermark, to_msk_naive_iso


def test_timestamptz_utc_to_msk_naive_iso() -> None:
    # psycopg2: 14:00 MSK stored as 11:00 UTC
    utc = datetime(2026, 6, 30, 11, 0, 0, tzinfo=timezone.utc)
    assert to_msk_naive_iso(utc) == "2026-06-30 14:00:00"


def test_naive_assumed_msk() -> None:
    naive = datetime(2026, 6, 30, 14, 0, 0)
    assert to_msk_naive_iso(naive) == "2026-06-30 14:00:00"


def test_watermark_msk_iso() -> None:
    naive = datetime(2026, 6, 30, 14, 0, 0)
    assert format_msk_watermark(naive) == "2026-06-30T14:00:00+03:00"
    assert ensure_msk_aware(datetime(2026, 6, 30, 11, 0, tzinfo=timezone.utc)) == datetime(
        2026, 6, 30, 14, 0, tzinfo=MSK
    )
