from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from dwh.common.airflow_meta import (
    count_successful_fsd_watermarks,
    fsd_ingest_dag_ids_missing_success,
    latest_dagrun_state,
)


def _mock_cursor(fetchone=None, fetchall=None):
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


@patch("dwh.common.airflow_meta._meta_connect")
def test_latest_dagrun_state_returns_row(mock_connect):
    conn = MagicMock()
    conn.cursor.return_value = _mock_cursor(fetchone=("success",))
    mock_connect.return_value = conn

    hour = datetime(2026, 7, 8, 12, tzinfo=timezone.utc)
    assert latest_dagrun_state("ingest_fsd_tickets", hour, hour) == "success"
    sql = conn.cursor.return_value.execute.call_args.args[0]
    assert "data_interval_end >= %s" in sql
    assert "data_interval_start" not in sql
    conn.close.assert_called_once()


@patch("dwh.common.airflow_meta._meta_connect")
def test_latest_dagrun_state_none_when_missing(mock_connect):
    conn = MagicMock()
    conn.cursor.return_value = _mock_cursor(fetchone=None)
    mock_connect.return_value = conn

    hour = datetime(2026, 7, 8, 12, tzinfo=timezone.utc)
    assert latest_dagrun_state("ingest_fsd_tickets", hour, hour) is None


@patch("dwh.common.airflow_meta.latest_dagrun_state", return_value="success")
def test_fsd_ingest_dag_ids_missing_success_empty(_mock_latest):
    hour = datetime(2026, 7, 8, 12, tzinfo=timezone.utc)
    assert fsd_ingest_dag_ids_missing_success(["ingest_fsd_a"], hour, hour) == []


@patch("dwh.common.airflow_meta.latest_dagrun_state", return_value=None)
def test_fsd_ingest_dag_ids_missing_success_lists_failed(_mock_latest):
    hour = datetime(2026, 7, 8, 12, tzinfo=timezone.utc)
    assert fsd_ingest_dag_ids_missing_success(["ingest_fsd_a", "ingest_fsd_b"], hour, hour) == [
        "ingest_fsd_a",
        "ingest_fsd_b",
    ]


@patch("dwh.common.airflow_meta._meta_connect")
def test_count_successful_fsd_watermarks(mock_connect):
    conn = MagicMock()
    conn.cursor.return_value = _mock_cursor(fetchone=(3,))
    mock_connect.return_value = conn

    hour = datetime(2026, 7, 8, 12, tzinfo=timezone.utc)
    assert count_successful_fsd_watermarks(hour, hour) == 3
