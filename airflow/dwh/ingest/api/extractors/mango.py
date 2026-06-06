"""PBX API extractors for bronze ingest."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from airflow.models import Variable

from dwh.common.api_ingest import load_api_watermark
from dwh.common.watermark import DEFAULT_OVERLAP_MINUTES, query_from

logger = logging.getLogger(__name__)

API_BASE_URL = "https://app.pbx-office.ru/vpbx/"
CC_BASE_URL = "https://app.pbx-office.ru/cc/"
# One CHUNK_DAYS window per DAG run; watermark advances to chunk end (or partial progress).
CHUNK_DAYS = 3
LIMIT = 5000
MAX_ATTEMPTS = 20
WAIT_SECONDS = 10
REQUEST_DELAY_SEC = 2
MAX_REPORT_REQUESTS_PER_RUN = 24
API_RETRY_MAX = 5
API_RETRY_BASE_SEC = 30
# PBX API dates are Moscow local wall clock (legacy: utc + 3h formatted as naive).
MSK = timezone(timedelta(hours=3))
# Benign API result codes: no data / invalid period for empty history windows.
_SKIP_REQUEST_RESULTS = {3104, 3300, 4000}
# Server overload / transient errors (PBXOfficeError 500x).
_RETRY_RESULTS = {5000, 5001, 5002, 5003, 5004}
# Classic stats CSV columns (order matters for headerless parsing fallback).
# entry_id + create — join key for extended stats / realtime.
_BASIC_STATS_FIELDS = (
    "records,start,finish,answer,from_extension,from_number,"
    "to_extension,to_number,disconnect_reason,line_number,location,entry_id,create"
)
_DEAL_STATUSES = ("in_work", "succeed", "failed", "delete")


class _ReportRequestBudget:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0
        self.exhausted = False
        self.max_end: datetime | None = None

    def mark_range_done(self, end: datetime) -> None:
        end_utc = end.astimezone(timezone.utc)
        if self.max_end is None or end_utc > self.max_end.astimezone(timezone.utc):
            self.max_end = end

    def try_acquire(self, label: str) -> bool:
        if self.used >= self.limit:
            self.exhausted = True
            logger.warning(
                "PBX stats/calls/request budget exhausted (%s/run) before %s",
                self.limit,
                label,
            )
            return False
        self.used += 1
        return True


def _sign(api_key: str, command: dict, api_salt: str) -> str:
    json_string = json.dumps(command, separators=(",", ":"))
    return hashlib.sha256(f"{api_key}{json_string}{api_salt}".encode("utf-8")).hexdigest()


def _request(
    endpoint: str,
    command: dict[str, Any],
    *,
    attempt: int = 0,
    base_url: str = API_BASE_URL,
    expect_json: bool = True,
) -> dict | str:
    api_key = Variable.get("pbx_api_key")
    api_salt = Variable.get("pbx_api_salt")
    endpoint = endpoint if endpoint.endswith("/") else f"{endpoint}/"
    json_string = json.dumps(command, separators=(",", ":"))
    try:
        response = requests.post(
            f"{base_url}{endpoint}",
            data={"vpbx_api_key": api_key, "sign": _sign(api_key, command, api_salt), "json": json_string},
            headers={"User-Agent": "AirflowDWH/1.0"},
            timeout=90,
            allow_redirects=False,
        )
        if response.status_code in (429, 503):
            raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
        # Classic stats/result: 204 = not ready, 200 = CSV body
        if not expect_json:
            return {"_http_status": response.status_code, "_text": response.text}
        if response.status_code >= 400:
            body = (response.text or "")[:500]
            # Do not retry 4xx (except rate-limit) — keep body for diagnostics
            if 400 <= response.status_code < 500 and response.status_code not in (429,):
                raise ValueError(f"PBX HTTP {response.status_code} on {endpoint}: {body}")
            response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        if isinstance(exc, ValueError) and "PBX HTTP 4" in str(exc):
            raise
        if attempt >= API_RETRY_MAX:
            raise
        delay = API_RETRY_BASE_SEC * (2**attempt)
        logger.warning("PBX HTTP error on %s (attempt %s): %s; sleep %ss", endpoint, attempt + 1, exc, delay)
        time.sleep(delay)
        return _request(
            endpoint, command, attempt=attempt + 1, base_url=base_url, expect_json=expect_json
        )

    if not expect_json:
        return payload  # type: ignore[return-value]

    result = payload.get("result")
    if result in _RETRY_RESULTS and attempt < API_RETRY_MAX:
        delay = API_RETRY_BASE_SEC * (2**attempt)
        logger.warning(
            "PBX overload result=%s on %s (attempt %s); sleep %ss payload=%s",
            result,
            endpoint,
            attempt + 1,
            delay,
            payload,
        )
        time.sleep(delay)
        return _request(
            endpoint, command, attempt=attempt + 1, base_url=base_url, expect_json=expect_json
        )
    return payload


def _request_cc(endpoint: str, command: dict[str, Any]) -> dict:
    return _request(endpoint, command, base_url=CC_BASE_URL)  # type: ignore[return-value]


def _format_pbx_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MSK).strftime("%d.%m.%Y %H:%M:%S")


def _pbx_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(MSK)


def _request_report_key(command: dict[str, Any], budget: _ReportRequestBudget) -> str | None:
    label = f"{command.get('start_date')}..{command.get('end_date')}"
    if not budget.try_acquire(label):
        return None
    if REQUEST_DELAY_SEC > 0:
        time.sleep(REQUEST_DELAY_SEC)
    payload = _request("stats/calls/request", command)
    key = payload.get("key")
    if key:
        return key
    result = payload.get("result")
    if result in _SKIP_REQUEST_RESULTS:
        logger.info(
            "PBX stats/calls/request skipped for %s..%s: result=%s payload=%s",
            command.get("start_date"),
            command.get("end_date"),
            result,
            payload,
        )
        return None
    raise ValueError(f"PBX stats/calls/request failed: {payload}")


def _to_dt(value: Any) -> datetime | None:
    """PBX API uses context_start_time (unix seconds), not started_at."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return datetime.fromtimestamp(int(stripped), tz=timezone.utc)
        try:
            return datetime.fromtimestamp(float(stripped), tz=timezone.utc)
        except ValueError:
            parsed = pd.to_datetime(stripped, utc=True, errors="coerce")
            if pd.isna(parsed):
                return None
            return parsed.to_pydatetime()
    return None


def _call_started_at(call: dict) -> datetime | None:
    # Legacy pbx_api_calls.py: to_timestamp(context_start_time)
    return _to_dt(call.get("context_start_time") or call.get("started_at"))


def _wait_report(key: str) -> list[dict]:
    for attempt in range(MAX_ATTEMPTS):
        payload = _request("stats/calls/result", {"key": key})
        status = payload.get("status")
        if status == "complete":
            calls: list[dict] = []
            for item in payload.get("data", []):
                period = item.get("period")
                for call in item.get("list", []):
                    call["period"] = period
                    calls.append(call)
            return calls
        if status == "error":
            raise ValueError(f"PBX report generation failed: {payload}")
        if status in ("request", "work"):
            logger.info("PBX report %s status=%s, wait %ss (%s/%s)", key, status, WAIT_SECONDS, attempt + 1, MAX_ATTEMPTS)
        time.sleep(WAIT_SECONDS)
    raise TimeoutError(f"PBX report {key} was not ready")


def _collect_recording_ids(call: dict[str, Any]) -> list[Any]:
    """Recording IDs: top-level recording_ids or recording_id from legs/members."""
    ids: list[Any] = []
    top = call.get("recording_ids")
    if isinstance(top, list):
        ids.extend(top)
    elif top not in (None, "", []):
        ids.append(top)
    for leg in call.get("context_calls") or []:
        if not isinstance(leg, dict):
            continue
        leg_ids = leg.get("recording_id") or []
        if isinstance(leg_ids, list):
            ids.extend(leg_ids)
        elif leg_ids not in (None, "", []):
            ids.append(leg_ids)
        for member in leg.get("members") or []:
            if not isinstance(member, dict):
                continue
            member_ids = member.get("recording_id") or []
            if isinstance(member_ids, list):
                ids.extend(member_ids)
            elif member_ids not in (None, "", []):
                ids.append(member_ids)
    # Preserve order, drop duplicates.
    seen: set[str] = set()
    out: list[Any] = []
    for item in ids:
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _fetch_calls_range(start: datetime, end: datetime, budget: _ReportRequestBudget) -> list[dict]:
    """Fetch calls for [start, end); split recursively when API returns LIMIT rows."""
    if start >= end:
        return []

    command = {
        "start_date": _format_pbx_dt(start),
        "end_date": _format_pbx_dt(end),
        "limit": str(LIMIT),
        "offset": "0",
        # Contact-center case data (assign/close user, result) in CallContext.conversion
        "ext_params": 1,
        "ext_fields": ["context_cost_full", "context_cost_tariff"],
    }
    key = _request_report_key(command, budget)
    if not key:
        if budget.exhausted:
            return []
        budget.mark_range_done(end)
        return []

    batch = _wait_report(key)
    if not batch:
        budget.mark_range_done(end)
        return []

    if len(batch) >= LIMIT and end - start > timedelta(minutes=1):
        mid = start + (end - start) / 2
        left = _fetch_calls_range(start, mid, budget)
        if budget.exhausted:
            return left
        right = _fetch_calls_range(mid, end, budget)
        return left + right
    budget.mark_range_done(end)
    return batch


def _window_for_run(cutoff: datetime, end_time: datetime) -> tuple[datetime, datetime]:
    """Single chunk per run: from watermark (minus overlap) up to +CHUNK_DAYS or now."""
    chunk_start = query_from(cutoff).astimezone(MSK)
    chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_time)
    return min(chunk_start, end_time), chunk_end


def extract_calls() -> pd.DataFrame:
    cutoff = load_api_watermark("pbx", "calls")
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    end_time = _pbx_now()
    chunk_start, chunk_end = _window_for_run(cutoff, end_time)
    budget = _ReportRequestBudget(MAX_REPORT_REQUESTS_PER_RUN)

    logger.info(
        "PBX extract chunk %s .. %s (watermark=%s, overlap=%sm, budget=%s requests)",
        _format_pbx_dt(chunk_start),
        _format_pbx_dt(chunk_end),
        cutoff.isoformat(),
        DEFAULT_OVERLAP_MINUTES,
        budget.limit,
    )

    calls: list[dict] = []
    if chunk_start < chunk_end:
        calls = _fetch_calls_range(chunk_start, chunk_end, budget)

    logger.info("PBX chunk done: %s calls, %s stats/calls/request used", len(calls), budget.used)
    if budget.exhausted:
        logger.warning(
            "PBX request budget exhausted; partial chunk saved, next run continues from watermark"
        )

    rows = []
    for call in calls:
        rows.append(
            {
                "entry_id": call.get("entry_id"),
                "started_at": _call_started_at(call),
                "context_type": call.get("context_type"),
                "context_status": call.get("context_status"),
                "caller_id": call.get("caller_id"),
                "caller_name": call.get("caller_name"),
                "caller_number": call.get("caller_number"),
                "called_number": call.get("called_number"),
                "duration": call.get("duration"),
                "talk_duration": call.get("talk_duration"),
                "context_init_type": call.get("context_init_type"),
                "recall_status": call.get("recall_status"),
                "cost": call.get("cost"),
                "context_cost_full": call.get("context_cost_full"),
                "context_cost_tariff": call.get("context_cost_tariff"),
                "recording_ids": json.dumps(_collect_recording_ids(call), ensure_ascii=False),
                "period": call.get("period"),
                # conversion / tag_id / script_id / marks stay in raw_data (ext_params=1)
                "raw_data": json.dumps(call, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    if budget.max_end is not None:
        df.attrs["watermark_to"] = budget.max_end.astimezone(timezone.utc).isoformat()
    elif not budget.exhausted and chunk_start < chunk_end:
        # Chunk-based watermark: advance to chunk end when API returns 0 rows (empty history slice).
        df.attrs["watermark_to"] = chunk_end.astimezone(timezone.utc).isoformat()
    return df


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    text = str(value).strip()
    return text or None


def extract_users() -> pd.DataFrame:
    """VPBX employees: POST /config/users/request."""
    payload = _request(
        "config/users/request",
        {
            "ext_fields": [
                "general.user_id",
                "general.sips",
                "groups",
                "general.access_role_id",
            ]
        },
    )
    users = payload.get("users") or payload.get("data") or []
    if isinstance(users, dict):
        users = list(users.values())
    rows = []
    for user in users:
        general = user.get("general") if isinstance(user.get("general"), dict) else {}
        telephony = user.get("telephony") if isinstance(user.get("telephony"), dict) else {}
        extension = user.get("extension") or general.get("extension") or telephony.get("extension")
        if extension is None and user.get("user_id") is None and not general.get("user_id"):
            # keep row keyed by name fallback
            extension = user.get("name") or general.get("name")
        rows.append(
            {
                "extension": _as_str(extension),
                "name": _as_str(user.get("name") or general.get("name")),
                "email": _as_str(user.get("email") or general.get("email")),
                "department": _as_str(user.get("department") or general.get("department")),
                "position": _as_str(user.get("position") or general.get("position")),
                "user_id": _as_str(user.get("user_id") or general.get("user_id")),
                "access_role_id": _as_str(
                    user.get("access_role_id") or general.get("access_role_id")
                ),
                "mobile": _as_str(user.get("mobile") or general.get("mobile")),
                "groups": json.dumps(user.get("groups") or [], ensure_ascii=False),
                "sips": json.dumps(
                    user.get("sips") or general.get("sips") or [], ensure_ascii=False
                ),
                "raw_data": json.dumps(user, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["extension"].notna() & (df["extension"] != "")]
        df = df.drop_duplicates(subset=["extension"], keep="last")
    return df


def extract_groups() -> pd.DataFrame:
    payload = _request("groups", {"show_users": 1})
    groups = payload.get("groups") or payload.get("data") or []
    if isinstance(groups, dict):
        groups = list(groups.values())
    rows = []
    for group in groups:
        group_id = group.get("group_id") or group.get("id")
        rows.append(
            {
                "group_id": _as_str(group_id),
                "name": _as_str(group.get("name")),
                "extension": _as_str(group.get("extension")),
                "operators": json.dumps(group.get("operators") or [], ensure_ascii=False),
                "raw_data": json.dumps(group, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["group_id"].notna() & (df["group_id"] != "")]
    return df


def extract_numbers() -> pd.DataFrame:
    """Incoming lines / DID: POST /incominglines."""
    payload = _request("incominglines", {})
    lines = payload.get("lines") or payload.get("data") or []
    rows = []
    for line in lines:
        line_id = line.get("line_id") or line.get("id")
        rows.append(
            {
                "line_id": _as_str(line_id),
                "number": _as_str(line.get("number")),
                "name": _as_str(line.get("name")),
                "comment": _as_str(line.get("comment")),
                "region": _as_str(line.get("region")),
                "schema_id": _as_str(line.get("schema_id")),
                "schema_name": _as_str(line.get("schema_name")),
                "raw_data": json.dumps(line, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["line_id"].notna() & (df["line_id"] != "")]
    return df


def extract_sip() -> pd.DataFrame:
    """SIP accounts — try config/sip then config/sip/request."""
    payload: dict[str, Any] = {}
    last_err: Exception | None = None
    for endpoint in ("config/sip", "config/sip/request"):
        try:
            payload = _request(endpoint, {})
            if payload.get("result") in _SKIP_REQUEST_RESULTS and not (
                payload.get("sips") or payload.get("users") or payload.get("data")
            ):
                continue
            break
        except Exception as exc:  # noqa: BLE001 — fallback endpoints
            last_err = exc
            logger.warning("PBX %s failed: %s", endpoint, exc)
            payload = {}
    if not payload and last_err:
        raise last_err

    sips = payload.get("sips") or payload.get("users") or payload.get("data") or []
    if isinstance(sips, dict):
        sips = list(sips.values())
    rows = []
    for item in sips:
        login = (
            item.get("login")
            or item.get("sip_login")
            or item.get("name")
            or (item.get("general") or {}).get("login")
        )
        rows.append(
            {
                "login": _as_str(login),
                "user_id": _as_str(item.get("user_id") or (item.get("general") or {}).get("user_id")),
                "extension": _as_str(
                    item.get("extension") or (item.get("general") or {}).get("extension")
                ),
                "name": _as_str(item.get("name") or (item.get("general") or {}).get("name")),
                "raw_data": json.dumps(item, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["login"].notna() & (df["login"] != "")]
        df = df.drop_duplicates(subset=["login"], keep="last")
    return df


def _unix_msk(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(MSK).timestamp())


def _wait_basic_stats_csv(key: str) -> str:
    """Poll classic stats/result until CSV body (HTTP 200) or give up."""
    for attempt in range(MAX_ATTEMPTS):
        payload = _request("stats/result", {"key": key}, expect_json=False)
        assert isinstance(payload, dict)
        status = int(payload.get("_http_status") or 0)
        text = str(payload.get("_text") or "")
        if status == 200 and text.strip():
            return text
        if status == 404:
            raise ValueError(f"PBX stats/result key not found: {key}")
        if status not in (200, 204):
            logger.warning("PBX stats/result unexpected HTTP %s: %s", status, text[:300])
        time.sleep(WAIT_SECONDS)
    raise TimeoutError(f"PBX classic stats {key} was not ready")


def _parse_basic_stats_csv(text: str) -> list[dict[str, Any]]:
    import csv
    from io import StringIO

    fieldnames = [f.strip() for f in _BASIC_STATS_FIELDS.split(",")]
    first_line = (text.splitlines() or [""])[0]
    if any(h in first_line.lower() for h in ("records", "entry_id", "from_extension")):
        dict_reader = csv.DictReader(StringIO(text), delimiter=";")
        return [{k: (v if v != "" else None) for k, v in rec.items()} for rec in dict_reader]

    rows_out: list[dict[str, Any]] = []
    for parts in csv.reader(StringIO(text), delimiter=";"):
        if not parts or all(not (p or "").strip() for p in parts):
            continue
        mapped = {name: (parts[idx] if idx < len(parts) else None) for idx, name in enumerate(fieldnames)}
        rows_out.append(mapped)
    return rows_out


def extract_call_stats_basic() -> pd.DataFrame:
    """Classic CSV stats: from/to extension + disconnect_reason (watermarked)."""
    cutoff = load_api_watermark("pbx", "call_stats_basic")
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    end_time = _pbx_now()
    # Classic stats: period ≤ 1 month; epoch watermark returns HTTP 400.
    max_lookback = end_time - timedelta(days=28)
    if cutoff.astimezone(MSK) < max_lookback:
        logger.info(
            "PBX call_stats_basic: clamp watermark %s → %s (API max ~1 month)",
            cutoff.isoformat(),
            max_lookback.isoformat(),
        )
        cutoff = max_lookback
    chunk_start, chunk_end = _window_for_run(cutoff, end_time)
    if chunk_start >= chunk_end:
        return pd.DataFrame()

    # As in vendor sample: from/to with empty filters; timestamps as strings.
    command = {
        "date_from": str(_unix_msk(chunk_start)),
        "date_to": str(_unix_msk(chunk_end)),
        "from": {"extension": "", "number": ""},
        "to": {"extension": "", "number": ""},
        "fields": _BASIC_STATS_FIELDS,
    }
    logger.info(
        "PBX call_stats_basic request %s .. %s",
        command["date_from"],
        command["date_to"],
    )
    payload = _request("stats/request", command)
    assert isinstance(payload, dict)
    key = payload.get("key")
    if not key:
        if payload.get("result") in _SKIP_REQUEST_RESULTS:
            df = pd.DataFrame()
            df.attrs["watermark_to"] = chunk_end.astimezone(timezone.utc).isoformat()
            return df
        raise ValueError(f"PBX stats/request failed: {payload}")

    csv_text = _wait_basic_stats_csv(str(key))
    parsed = _parse_basic_stats_csv(csv_text)
    rows = []
    for rec in parsed:
        start_raw = rec.get("start")
        # start is unix seconds in classic stats
        started_at = None
        try:
            ts = int(float(str(start_raw)))
            if ts > 0:
                started_at = datetime.fromtimestamp(ts, tz=MSK).astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            started_at = _as_str(start_raw)
        rows.append(
            {
                "entry_id": _as_str(rec.get("entry_id")),
                "started_at": started_at,
                "finish": _as_str(rec.get("finish")),
                "answer": _as_str(rec.get("answer")),
                "from_extension": _as_str(rec.get("from_extension")),
                "from_number": _as_str(rec.get("from_number")),
                "to_extension": _as_str(rec.get("to_extension")),
                "to_number": _as_str(rec.get("to_number")),
                "disconnect_reason": _as_str(rec.get("disconnect_reason")),
                "line_number": _as_str(rec.get("line_number")),
                "location": _as_str(rec.get("location")),
                "records": _as_str(rec.get("records")),
                "create_ts": _as_str(rec.get("create")),
                "raw_data": json.dumps(rec, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    df.attrs["watermark_to"] = chunk_end.astimezone(timezone.utc).isoformat()
    logger.info("PBX call_stats_basic: %s rows for %s..%s", len(df), chunk_start, chunk_end)
    return df


def extract_deals() -> pd.DataFrame:
    """CC deals snapshot: POST /cc/deal/list for each status."""
    rows: list[dict[str, Any]] = []
    for status in _DEAL_STATUSES:
        try:
            payload = _request_cc("deal/list", {"status": status})
        except Exception as exc:  # noqa: BLE001
            logger.warning("PBX CC deal/list status=%s failed: %s", status, exc)
            continue
        assert isinstance(payload, dict)
        deals = payload.get("deals") or payload.get("data") or []
        if isinstance(deals, dict):
            deals = list(deals.values())
        for deal in deals:
            deal_id = deal.get("deal_id") or deal.get("deals_ids") or deal.get("id")
            rows.append(
                {
                    "deal_id": _as_str(deal_id),
                    "name": _as_str(deal.get("name")),
                    "description": _as_str(deal.get("description")),
                    "amount": deal.get("amount"),
                    "abonent_id": _as_str(deal.get("abonent_id")),
                    "contact_id": _as_str(deal.get("contact_id")),
                    "funnel_id": _as_str(deal.get("funnel_id")),
                    "step_id": _as_str(deal.get("step_id")),
                    "status": _as_str(deal.get("status") or status),
                    "status_change_reason": _as_str(deal.get("status_change_reason")),
                    "reason_comment": _as_str(deal.get("reason_comment")),
                    "custom_fields": json.dumps(deal.get("custom_fields"), ensure_ascii=False)
                    if deal.get("custom_fields") is not None
                    else None,
                    "raw_data": json.dumps(deal, ensure_ascii=False),
                }
            )
        time.sleep(REQUEST_DELAY_SEC)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["deal_id"].notna() & (df["deal_id"] != "")]
        df = df.drop_duplicates(subset=["deal_id"], keep="last")
    return df


def extract_tasks() -> pd.DataFrame:
    """CC tasks: POST /cc/task/list (paginated)."""
    rows: list[dict[str, Any]] = []
    offset = 0
    limit = 500
    while True:
        payload = _request_cc("task/list", {"limit": limit, "offset": offset})
        assert isinstance(payload, dict)
        tasks = (
            payload.get("tasks")
            or payload.get("data")
            or payload.get("list")
            or []
        )
        if isinstance(tasks, dict):
            tasks = list(tasks.values())
        if not tasks:
            break
        for task in tasks:
            task_id = task.get("task_id") or task.get("id")
            rows.append(
                {
                    "task_id": _as_str(task_id),
                    "status": _as_str(task.get("status")),
                    "event_type": _as_str(task.get("event_type")),
                    "priority": _as_str(task.get("priority")),
                    "contact_id": _as_str(task.get("contact_id")),
                    "deal_id": _as_str(task.get("deal_id")),
                    "from_user_id": _as_str(task.get("from_user_id")),
                    "to_user_id": _as_str(task.get("to_user_id")),
                    "start_time": _as_str(task.get("start_time")),
                    "raw_data": json.dumps(task, ensure_ascii=False),
                }
            )
        if len(tasks) < limit:
            break
        offset += limit
        time.sleep(REQUEST_DELAY_SEC)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["task_id"].notna() & (df["task_id"] != "")]
        df = df.drop_duplicates(subset=["task_id"], keep="last")
    return df


def extract_dialer_campaigns() -> pd.DataFrame:
    """Outbound dialer campaigns: POST /vpbx/campaign/list."""
    payload = _request("campaign/list", {})
    assert isinstance(payload, dict)
    campaigns = payload.get("campaigns") or payload.get("data") or []
    if isinstance(campaigns, dict):
        campaigns = list(campaigns.values())
    rows = []
    for c in campaigns:
        campaign_id = c.get("campaign_id") or c.get("id")
        created_by = c.get("created_by") if isinstance(c.get("created_by"), dict) else {}
        rows.append(
            {
                "campaign_id": _as_str(campaign_id),
                "name": _as_str(c.get("name")),
                "status": _as_str(c.get("status")),
                "priority": _as_str(c.get("priority")),
                "line_id": _as_str(c.get("line_id")),
                "start_ts": _as_str(c.get("start")),
                "end_ts": _as_str(c.get("end")),
                "created_ts": _as_str(c.get("created")),
                "created_by_user_id": _as_str(created_by.get("user_id")),
                "tasks_count": c.get("tasks_count"),
                "finished_tasks_count": c.get("finished_tasks_count"),
                "members": json.dumps(c.get("members") or [], ensure_ascii=False),
                "operators": json.dumps(c.get("operators") or [], ensure_ascii=False),
                "raw_data": json.dumps(c, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["campaign_id"].notna() & (df["campaign_id"] != "")]
        df = df.drop_duplicates(subset=["campaign_id"], keep="last")
    return df


def extract_dialer_tasks() -> pd.DataFrame:
    """Dialer campaign tasks: POST /vpbx/v2/campaign/tasks for each campaign."""
    camps = extract_dialer_campaigns()
    if camps.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for campaign_id in camps["campaign_id"].tolist():
        cursor = None
        for _page in range(50):
            cmd: dict[str, Any] = {
                "campaign_ids": [int(campaign_id) if str(campaign_id).isdigit() else campaign_id],
                "fields": ["alias_task"],
                "limit": 500,
            }
            if cursor is not None:
                cmd["cursor"] = cursor
            try:
                payload = _request("v2/campaign/tasks", cmd)
            except Exception as exc:  # noqa: BLE001
                logger.warning("PBX v2/campaign/tasks campaign=%s failed: %s", campaign_id, exc)
                break
            assert isinstance(payload, dict)
            tasks = payload.get("tasks") or payload.get("data") or []
            if isinstance(tasks, dict):
                tasks = list(tasks.values())
            for task in tasks:
                task_id = task.get("task_id") or task.get("campaign_task_id") or task.get("id")
                rows.append(
                    {
                        "task_id": _as_str(task_id),
                        "campaign_id": _as_str(task.get("campaign_id") or campaign_id),
                        "number": _as_str(task.get("number")),
                        "name": _as_str(task.get("name")),
                        "task_status": _as_str(task.get("task_status") or task.get("status")),
                        "operator_id": _as_str(task.get("operator_id")),
                        "attempts_count": task.get("attempts_count"),
                        "task_created": _as_str(task.get("task_created")),
                        "task_updated": _as_str(task.get("task_updated")),
                        "raw_data": json.dumps(task, ensure_ascii=False),
                    }
                )
            cursor = payload.get("cursor") or payload.get("next_cursor")
            if not tasks or not cursor:
                break
            time.sleep(REQUEST_DELAY_SEC)
        time.sleep(REQUEST_DELAY_SEC)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["task_id"].notna() & (df["task_id"] != "")]
        df = df.drop_duplicates(subset=["task_id"], keep="last")
    return df


EXTRACTORS = {
    "calls": extract_calls,
    "call_stats_basic": extract_call_stats_basic,
    "users": extract_users,
    "groups": extract_groups,
    "numbers": extract_numbers,
    "sip": extract_sip,
    "deals": extract_deals,
    "tasks": extract_tasks,
    "dialer_campaigns": extract_dialer_campaigns,
    "dialer_tasks": extract_dialer_tasks,
}
