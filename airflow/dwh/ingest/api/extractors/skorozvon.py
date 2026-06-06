"""Dialer API extractors for bronze ingest."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from airflow.models import Variable

from dwh.common.api_ingest import load_api_watermark
from dwh.common.watermark import DEFAULT_OVERLAP_MINUTES, DEFAULT_WATERMARK, query_from

logger = logging.getLogger(__name__)

CALLS_URL = "https://api.dialer.ru/api/reports/calls_total.json"
HOURS_URL = "https://api.dialer.ru/api/reports/managers_employment.json"
PAGE_SIZE = 10000
HOURS_PAGE_SIZE = 1000
# esp_scorozvon_calls / esp_scorozvon_hours constants
LOOKBACK_DAYS = 1
API_DELAY_HOURS = 0
HOURS_LOOKBACK_HOURS = 24
HOURS_API_DELAY_HOURS = -3
HOURS_MAX_PER_RUN = 24 * 7
HOURS_REQUEST_DELAY_SEC = 2
REQUEST_DELAY_SEC = 5
API_RETRY_MAX = 10
API_RETRY_SLEEP_SEC = 180
LEGACY_RETRY_MAX = 3
LEGACY_RETRY_SLEEP_SEC = 10

_EPOCH = datetime.fromisoformat(DEFAULT_WATERMARK.replace("Z", "+00:00"))


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {Variable.get('dialer_access_token')}",
        "Content-Type": "application/json",
    }


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _unix_time(dt: datetime) -> int:
    """esp_scorozvon_calls.unix_time — plain timestamp(), no timezone tricks."""
    return int(_ensure_utc(dt).timestamp())


def _dt_str(dt: datetime) -> str:
    """esp_scorozvon_hours filter.range — strftime on UTC-labelled datetimes."""
    return _ensure_utc(dt).strftime("%Y-%m-%d %H:%M:%S")


def _retry_sleep_seconds(response: requests.Response | None, attempt: int) -> int:
    if response is not None and response.status_code == 429:
        raw = response.headers.get("Retry-After")
        if raw and str(raw).isdigit():
            return max(int(raw), API_RETRY_SLEEP_SEC)
        return API_RETRY_SLEEP_SEC
    return LEGACY_RETRY_SLEEP_SEC


def _post(url: str, payload: dict[str, Any], *, label: str = "") -> dict:
    last_error: Exception | None = None
    last_response: requests.Response | None = None
    max_attempts = API_RETRY_MAX if url == CALLS_URL else LEGACY_RETRY_MAX
    for attempt in range(max_attempts):
        try:
            response = requests.post(url, headers=_headers(), json=payload, timeout=60)
            last_response = response
            if not response.ok:
                body = response.text[:2000]
                logger.error(
                    "Dialer API HTTP %s label=%s url=%s payload=%s body=%s",
                    response.status_code,
                    label or "-",
                    url,
                    json.dumps(payload, ensure_ascii=False),
                    body,
                )
                response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 >= max_attempts:
                break
            delay = _retry_sleep_seconds(last_response, attempt)
            logger.warning(
                "Dialer API request failed label=%s (attempt %s/%s): %s; sleep %ss",
                label or "-",
                attempt + 1,
                max_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> str | None:
    return str(value) if value is not None else None


def _parse_hours_item_date(raw_date: Any, hour_start: datetime) -> str:
    """esp_scorozvon_hours: API date is dd.mm.yyyy, fallback to hour_start.date()."""
    if raw_date is None:
        return hour_start.date().isoformat()
    text = str(raw_date).strip()
    if not text:
        return hour_start.date().isoformat()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt).date().isoformat()
        except ValueError:
            continue
    return hour_start.date().isoformat()


def _safe_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def _map_call(item: dict[str, Any]) -> dict[str, Any]:
    user = item.get("user") or {}
    durations = item.get("durations") or {}
    source = item.get("source")
    return {
        "id": item.get("id"),
        "started_at": item.get("started_at"),
        "region": item.get("region"),
        "phone": item.get("phone"),
        "call_type": item.get("call_type"),
        "call_type_code": item.get("call_type_code"),
        "missed_reason": item.get("missed_reason"),
        "duration": _safe_int(item.get("duration")),
        "recording_url": item.get("recording_url"),
        "source": str(source) if source is not None else None,
        "waiting_time": _safe_int(item.get("waiting_time")),
        "waiting_on_line_time": _safe_float(item.get("waiting_on_line_time")),
        "terminator": item.get("terminator"),
        "cost": _safe_float(item.get("cost")),
        "duration_without_holding": _safe_int(durations.get("without_holding")),
        "duration_holding": _safe_int(durations.get("holding")),
        "duration_billed": _safe_float(durations.get("billed")),
        "user_id": user.get("id"),
        "user_name": user.get("name"),
        "organization": _safe_json(item.get("organization")),
        "lead": _safe_json(item.get("lead")),
        "scenario": _safe_json(item.get("scenario")),
        "scenario_result": _safe_json(item.get("scenario_result")),
        "scenario_result_group": _safe_json(item.get("scenario_result_group")),
        "main_lead": _safe_json(item.get("main_lead")),
        "event": _safe_json(item.get("event")),
        "call_project": _safe_json(item.get("call_project")),
        "transfer_initiator": item.get("transfer_initiator"),
        "transfer_initiator_id": item.get("transfer_initiator_id"),
        "external_access_id": item.get("external_access_id"),
        "reason": item.get("reason"),
        "raw_data": json.dumps(item, ensure_ascii=False),
    }


def _calls_payload(page: int, start_time: datetime, end_time: datetime) -> dict[str, Any]:
    return {
        "length": PAGE_SIZE,
        "page": page,
        "start_time": _unix_time(start_time),
        "end_time": _unix_time(end_time),
        "filter": {
            "results_ids": "all",
            "scenarios_ids": "all",
            "tags_ids": "all",
            "types": "all",
            "users_ids": "all",
        },
    }


def _fetch_calls(start_time: datetime, end_time: datetime) -> list[dict]:
    """Pagination loop from esp_scorozvon_calls.load_calls_incremental."""
    rows: list[dict] = []
    page = 1
    total_pages = None

    while True:
        payload = _calls_payload(page, start_time, end_time)
        label = (
            f"calls page={page} "
            f"start_time={payload['start_time']} end_time={payload['end_time']}"
        )
        if page > 1 and REQUEST_DELAY_SEC > 0:
            time.sleep(REQUEST_DELAY_SEC)
        result = _post(CALLS_URL, payload, label=label)
        calls = result.get("data", [])
        if total_pages is None:
            total_pages = result.get("total_pages", 1)
        logger.info(
            "Dialer calls page=%s/%s rows=%s total=%s unix=%s..%s",
            page,
            total_pages,
            len(calls),
            result.get("total"),
            payload["start_time"],
            payload["end_time"],
        )
        if not calls:
            break
        rows.extend(calls)
        if page >= total_pages:
            break
        page += 1
    return rows


def extract_calls() -> pd.DataFrame:
    """Incremental load: watermark ~= MAX(started_at), window [watermark-30m, now]."""
    api_now = datetime.now(timezone.utc) - timedelta(hours=API_DELAY_HOURS)
    watermark = _ensure_utc(load_api_watermark("dialer", "calls"))

    if watermark <= _EPOCH + timedelta(days=1):
        last_started_at = api_now - timedelta(days=LOOKBACK_DAYS)
    else:
        last_started_at = watermark

    start_time = query_from(last_started_at)
    end_time = api_now

    if start_time >= end_time:
        logger.info("Dialer calls: nothing to fetch (start=%s end=%s)", start_time, end_time)
        return pd.DataFrame()

    logger.info(
        "Dialer calls window %s .. %s (watermark=%s, overlap=%sm)",
        start_time.isoformat(),
        end_time.isoformat(),
        watermark.isoformat(),
        DEFAULT_OVERLAP_MINUTES,
    )

    rows = _fetch_calls(start_time, end_time)
    mapped = [_map_call(item) for item in rows]
    df = pd.DataFrame(mapped)
    if not df.empty and "started_at" in df.columns:
        df["started_at"] = pd.to_datetime(df["started_at"], utc=True, errors="coerce")
        max_ts = df["started_at"].max()
        if pd.notna(max_ts):
            df.attrs["watermark_to"] = max_ts.isoformat()

    logger.info("Dialer calls done: %s rows", len(df))
    return df


def _map_hours_item(item: dict[str, Any], hour_start: datetime) -> dict[str, Any] | None:
    user_id = _safe_int(item.get("user_id"))
    if user_id is None or user_id <= 0:
        return None
    statuses = item.get("statuses") or {}
    performance = item.get("performance") or {}
    return {
        "date_h": hour_start,
        "date": _parse_hours_item_date(item.get("date"), hour_start),
        "user_id": user_id,
        "full_name": item.get("full_name"),
        "email": item.get("email"),
        "status_normal": _safe_int(statuses.get("normal")),
        "status_ringing": _safe_int(statuses.get("ringing")),
        "status_speaking": _safe_int(statuses.get("speaking")),
        "status_wrapup": _safe_int(statuses.get("wrapup")),
        "status_away": _safe_int(statuses.get("away")),
        "status_dnd": _safe_int(statuses.get("dnd")),
        "status_accident": _safe_int(statuses.get("accident")),
        "status_offline": _safe_int(statuses.get("offline")),
        "performance_seconds": _safe_int(performance.get("in_seconds")),
        "performance_hours": _safe_str(performance.get("in_hours")),
        "performance_occ": _safe_str(performance.get("occ")),
    }


def _fetch_hours_for_hour(hour_start: datetime, hour_end: datetime) -> list[dict[str, Any]] | None:
    page = 1
    total_pages = None
    hour_rows: list[dict[str, Any]] = []

    while True:
        label = f"hours page={page} hour={hour_start.isoformat()}"
        payload = {
            "limit": HOURS_PAGE_SIZE,
            "page": page,
            "filter": {
                "range": [
                    _dt_str(hour_start),
                    _dt_str(hour_end),
                ]
            },
        }
        try:
            result = _post(HOURS_URL, payload, label=label)
        except requests.RequestException as exc:
            logger.error("Dialer hours skipped hour=%s: %s", hour_start.isoformat(), exc)
            return None

        data = result.get("data", [])
        if total_pages is None:
            total_pages = result.get("total_pages", 1)
        for item in data:
            mapped = _map_hours_item(item, hour_start)
            if mapped is not None:
                hour_rows.append(mapped)
        if page >= total_pages:
            break
        page += 1
        if HOURS_REQUEST_DELAY_SEC > 0:
            time.sleep(HOURS_REQUEST_DELAY_SEC)

    return hour_rows


def extract_hours() -> pd.DataFrame:
    """esp_scorozvon_hours parity with bounded catch-up: up to HOURS_MAX_PER_RUN hours per DAG run."""
    api_now = datetime.now(timezone.utc) + timedelta(hours=HOURS_API_DELAY_HOURS)
    end_time = api_now
    watermark = _ensure_utc(load_api_watermark("dialer", "hours"))

    fallback_start = api_now - timedelta(hours=HOURS_LOOKBACK_HOURS)
    last_date_h = watermark if watermark > _EPOCH + timedelta(days=1) else fallback_start
    start_time = query_from(last_date_h)

    if start_time >= end_time:
        logger.info("Dialer hours: nothing to fetch (start=%s end=%s)", start_time, end_time)
        df = pd.DataFrame()
        df.attrs["watermark_to"] = end_time.isoformat()
        return df

    run_end_time = min(
        end_time,
        start_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=HOURS_MAX_PER_RUN),
    )

    logger.info(
        "Dialer hours window %s .. %s (chunk to %s, watermark=%s, overlap=%sm)",
        start_time.isoformat(),
        end_time.isoformat(),
        run_end_time.isoformat(),
        watermark.isoformat(),
        DEFAULT_OVERLAP_MINUTES,
    )

    current_hour = start_time.replace(minute=0, second=0, microsecond=0)
    rows: list[dict] = []
    skipped_hours = 0

    while current_hour < run_end_time:
        hour_start = current_hour
        hour_end = current_hour + timedelta(hours=1) - timedelta(seconds=1)
        hour_rows = _fetch_hours_for_hour(hour_start, hour_end)
        if hour_rows is None:
            skipped_hours += 1
        else:
            rows.extend(hour_rows)
        current_hour += timedelta(hours=1)
        if HOURS_REQUEST_DELAY_SEC > 0 and current_hour < run_end_time:
            time.sleep(HOURS_REQUEST_DELAY_SEC)

    df = pd.DataFrame(rows)
    df.attrs["watermark_to"] = run_end_time.isoformat()
    logger.info(
        "Dialer hours done: %s rows, chunk_end=%s, skipped_hours=%s",
        len(df),
        run_end_time.isoformat(),
        skipped_hours,
    )
    return df


EXTRACTORS = {
    "calls": extract_calls,
    "hours": extract_hours,
}
