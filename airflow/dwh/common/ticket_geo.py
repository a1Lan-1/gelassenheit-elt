"""Incremental offline ticket geocoding for BI maps.

No geocode API. City centers live on S3 (uploaded once manually), not in git.

Incremental contract:
- Address source (temporary): cur_entities.legal_address.
- City ref: s3://gel-lake/ref/geo/city_centers.parquet (must already exist).
- Watermark: Airflow Variable dwh_watermark_fsd_ticket_geo (max source updated_at).
- Each run loads only tickets with updated_at > watermark - overlap.
- First run / force_full_refresh / empty variable: backfill by created_at lookback.
- Target: ReplicatedReplacingMergeTree(built_at), ORDER BY ticket_id.
- BI reads gel_helpdesk.mart_tickets_geo_v (FINAL).
"""

from __future__ import annotations

import io
import json
import logging
import re
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from dwh.common.ch_client import ch_execute
from dwh.common.config import s3_bucket
from dwh.common.constants import CH_DATABASE_HELPDESK, S3_BUCKET
from dwh.common.s3 import s3_client, upload_file_to_s3

logger = logging.getLogger(__name__)

S3_REF_PREFIX = "ref/geo"
CITY_CENTERS_KEY = f"{S3_REF_PREFIX}/city_centers.parquet"
CITY_MANIFEST_KEY = f"{S3_REF_PREFIX}/city_centers_manifest.json"
WATERMARK_VAR = "dwh_watermark_fsd_ticket_geo"
TARGET_TABLE = "mart_tickets_geo"
TARGET_VIEW = "mart_tickets_geo_v"
INSERT_BATCH_SIZE = 5000
DEFAULT_OVERLAP_MINUTES = 30

_CITY_PREFIX_RE = re.compile(
    # Do not treat \u0433.\u043e. / \u0441.\u043f. as "\u0433."+"\u043e". Capture only the first name token.
    r"(?i)(?:^|[\s,;])"
    r"(?:\u0433\.\u043e\.\s*|\u0433\s*\u043e\s+|\u0441\.\u043f\.\s*|\u0433\.\u043f\.\s*|\u043c\.\u043e\.\s*|\u043c\.\u0440-?\u043d\s*|"
    r"\u0433\.(?!\u043e)\s*|\u0433\s+|\u0433\u043e\u0440\u043e\u0434\s+|\u043f\u043e\u0441\.\s*|\u043f\u043e\u0441\s+|\u043f\u0433\u0442\.\s*|\u043f\u0433\u0442\s+|"
    r"\u0441\u0435\u043b\u043e\s+|\u0441\.(?!\u043f)\s*|\u0434\u0435\u0440\.\s*|\u0434\u0435\u0440\s+|\u0434\u0435\u0440\u0435\u0432\u043d\u044f\s+|"
    r"\u0441\u0442-\u0446\u0430\s+|\u0441\u0442\u0430\u043d\u0438\u0446\u0430\s+|\u0440\u043f\s+|\u0440\u043f\.\s+|\u0441\u043b\.\s+|\u0441\u043b\u043e\u0431\u043e\u0434\u0430\s+)"
    r"([A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u0451\-]+)"
)
# Compact: "\u0433.\u0422\u044e\u043c\u0435\u043d\u044c" / "\u0433.\u041a\u0440\u0430\u0441\u043d\u043e\u0434\u0430\u0440" (dot, no space). Not "\u0433.\u043e".
_CITY_COMPACT_RE = re.compile(
    r"(?i)(?:^|[\s,;])(?:\u0433\.(?!\u043e)|\u043f\u043e\u0441\.(?!\u043f)|\u043f\u0433\u0442\.|\u0434\u0435\u0440\.)"
    r"([A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u0451\-]+)"
)
_LEADING_TYPE_RE = re.compile(
    r"^(\u0433\.\u043e\.|\u0433\.?|\u0433\u043e\u0440\u043e\u0434|\u043f\u043e\u0441\.?|\u043f\u0433\u0442\.?|\u0441\u0435\u043b\u043e|\u0441\.|\u0434\u0435\u0440\.?|\u0434\u0435\u0440\u0435\u0432\u043d\u044f|\u0440\u043f\.?|\u0441\u0442-\u0446\u0430)\s+",
    re.IGNORECASE,
)
_TRAILING_TYPE_RE = re.compile(
    r"(?i)\s+(\u0433\.\u043e\.|\u0433\.|\u0433\u043e\u0440\u043e\u0434|\u0433|\u0440\u0435\u0441\u043f\.?|\u043e\u0431\u043b\.?|\u043a\u0440\u0430\u0439|\u0440\u0430\u0439\u043e\u043d|\u0440-\u043d|\u0443\u043b\.?|\u043f\u0440-\u043a\u0442|\u0448\.?)$"
)
_BAD_CITY_TOKENS = {
    "",
    "nan",
    "none",
    "nat",
    "<na>",
    "null",
    "unknown",
    "\u0440\u043e\u0441\u0441\u0438\u044f",
    "\u0440\u0444",
    "russia",
    "\u0430\u0434\u0440\u0435\u0441",
    "\u0442\u0435\u0441\u0442",
    "test",
}
_SKIP_TOKEN_RE = re.compile(
    r"(?i)"
    r"^(\u0440\u043e\u0441\u0441\u0438\u044f|\u0440\u0444|russia|\u0430\u0434\u0440\u0435\u0441|\u0442\u0435\u0441\u0442|test|unknown|\u0433|\u043e|\u043f|\u0441|\u0434|\u0443\u043b|\u043f\u0440|\u043f\u0435\u0440|\u0448|\u0441\u0442\u0440|"
    r"\u0433\.\u043e\.?|\u0441\.\u043f\.?|\u043c\.\u043e\.?|\u0440-\u043d|\u043e\u0431\u043b|\u043a\u0440\u0430\u0439|\u0440\u0435\u0441\u043f|\u043e\u043a\u0440\u0443\u0433)$"
)
_NON_CITY_TOKEN_RE = re.compile(
    r"(?i)"
    r"\u043a\u0440\u0430\u0439|\u043e\u0431\u043b\u0430\u0441\u0442\u044c|\b\u043e\u0431\u043b\.?\b|\u0440\u0430\u0439\u043e\u043d|\b\u0440-?\u043d\b|\u0443\u043b\u0438\u0446\u0430|\b\u0443\u043b\.|"
    r"\u043f\u0440\u043e\u0441\u043f\u0435\u043a\u0442|\u043f\u0440-\u043a\u0442|\u043f\u0435\u0440\u0435\u0443\u043b\u043e\u043a|\b\u043f\u0435\u0440\.|"
    r"\b\u0434\.|\u0434\u043e\u043c|\u043a\u043e\u0440\u043f\u0443\u0441|\b\u0441\u0442\u0440\.|\u043e\u0444\u0438\u0441|\u043f\u043e\u043c\u0435\u0449|\u0440\u0435\u0441\u043f\u0443\u0431\u043b\u0438\u043a\u0430|"
    r"\u0433\.\u043e\.|\u043c\.\u043e\.|\u0441\.\u043f\.|\u043e\u043a\u0440\u0443\u0433|\u043f\u043e\u0441\u0435\u043b\u043e\u043a|\u043f\u043e\u0441\u0451\u043b\u043e\u043a|\b\u043f\u0433\u0442\b|\u0431\u0443\u043b\u044c\u0432\u0430\u0440|\u0448\u043e\u0441\u0441\u0435|"
    r"\u043d\u0430\u0431\u0435\u0440\u0435\u0436\u043d\u0430\u044f|\u043f\u0440\u043e\u0435\u0437\u0434|\u0442\u0443\u043f\u0438\u043a|\u043c\u0438\u043a\u0440\u043e\u0440\u0430\u0439\u043e\u043d|\b\u043c\u043a\u0440\.|\u043a\u0432\u0430\u0440\u0442\u0430\u043b|"
    r"\u0444\u0435\u0434\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u044f|\u044d\u043b\u0435\u043c\u0435\u043d\u0442"
)


def _s3_uri(key: str) -> str:
    return f"s3://{s3_bucket() or S3_BUCKET}/{key}"


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _s3_get_json(key: str) -> dict[str, Any] | None:
    client = s3_client()
    bucket = s3_bucket() or S3_BUCKET
    try:
        from botocore.exceptions import ClientError

        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    except Exception as exc:
        if "NoSuchKey" in type(exc).__name__ or "404" in str(exc):
            return None
        raise
    return json.loads(obj["Body"].read().decode("utf-8"))


def _s3_put_json(key: str, payload: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        path = tmp.name
    try:
        upload_file_to_s3(path, _s3_uri(key))
    finally:
        Path(path).unlink(missing_ok=True)


def _s3_head(key: str) -> dict[str, Any] | None:
    """HEAD object; None if missing. Raises on auth/network errors (do not mask 403)."""
    client = s3_client()
    bucket = s3_bucket() or S3_BUCKET
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = ""
        status = None
        try:
            code = str(exc.response.get("Error", {}).get("Code", ""))  # type: ignore[attr-defined]
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")  # type: ignore[attr-defined]
        except Exception:
            pass
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return None
        raise


def _norm_name(value: Any) -> str:
    text = str(value or "").strip().lower().replace("\u0451", "\u0435")
    text = _LEADING_TYPE_RE.sub("", text)
    text = _TRAILING_TYPE_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip(" ,.;")
    # FIAS: "\u043d\u043e\u0432\u043e\u0441\u0438\u0431\u0438\u0440\u0441\u043a \u0433" / "\u043c\u043e\u0441\u043a\u0432\u0430 \u0433." leftover after trailing regex miss.
    text = re.sub(r"(?i)\s+\u0433\.?$", "", text)
    return text


def _candidate_keys(raw: str) -> list[str]:
    """Expand one token into lookup keys (suffixes, first word, hyphen city)."""
    key = _norm_name(raw)
    if not key or key in _BAD_CITY_TOKENS or _SKIP_TOKEN_RE.match(key):
        return []
    out: list[str] = [key]
    stripped = _TRAILING_TYPE_RE.sub("", key).strip()
    if stripped and stripped != key:
        out.append(stripped)
    # "\u0441\u0430\u043d\u043a\u0442-\u043f\u0435\u0442\u0435\u0440\u0431\u0443\u0440\u0433 \u0433" already handled; also "\u0444\u0435\u0434\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u044f \u043c\u043e\u0441\u043a\u0432\u0430"
    for prefix in ("\u0444\u0435\u0434\u0435\u0440\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u044f ", "\u0433\u043e\u0440\u043e\u0434 "):
        if key.startswith(prefix):
            rest = key[len(prefix) :].strip()
            if rest:
                out.append(rest)
    parts = key.replace("-", " ").split()
    if len(parts) >= 2:
        out.append(parts[0])
        out.append(" ".join(parts[:2]))
        out.append("-".join(parts[:2]))
        out.append("-".join(parts[:3]) if len(parts) >= 3 else "")
    return [x for x in out if x and x not in _BAD_CITY_TOKENS and len(x) >= 3]


def ensure_city_centers_on_s3(*, force: bool = False) -> dict[str, Any]:
    """Require city centers parquet on S3 (uploaded once offline). No internet download."""
    del force  # reserved for future re-upload tooling
    if _s3_head(CITY_CENTERS_KEY) is None:
        raise FileNotFoundError(
            f"Missing {_s3_uri(CITY_CENTERS_KEY)}. "
            "Upload once with: python -m dwh.scripts.upload_city_centers_to_s3 "
            "<path-to-city_centers.parquet|csv>"
        )
    manifest = _s3_get_json(CITY_MANIFEST_KEY) or {
        "s3_key": CITY_CENTERS_KEY,
        "source": "manual_s3_seed",
    }
    logger.info("city_centers present on S3 key=%s", CITY_CENTERS_KEY)
    return {"updated": False, "manifest": manifest}


def upload_city_centers_file(local_path: str | Path, *, source: str = "manual") -> dict[str, Any]:
    """One-shot: put local parquet/csv to S3 as the shared city-centers reference."""
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(path)

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            pq_path = Path(tmp.name)
        try:
            df.to_parquet(pq_path, index=False)
            upload_file_to_s3(str(pq_path), _s3_uri(CITY_CENTERS_KEY))
        finally:
            pq_path.unlink(missing_ok=True)
        row_count = int(len(df))
    elif path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
        upload_file_to_s3(str(path), _s3_uri(CITY_CENTERS_KEY))
        row_count = int(len(df))
    else:
        raise ValueError(f"Unsupported file type: {path.suffix} (use .parquet or .csv)")

    manifest = {
        "s3_key": CITY_CENTERS_KEY,
        "source": source,
        "local_name": path.name,
        "row_count": row_count,
        "uploaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _s3_put_json(CITY_MANIFEST_KEY, manifest)
    logger.info("uploaded city_centers rows=%s -> %s", row_count, _s3_uri(CITY_CENTERS_KEY))
    return manifest


def load_city_centers() -> pd.DataFrame:
    client = s3_client()
    bucket = s3_bucket() or S3_BUCKET
    obj = client.get_object(Bucket=bucket, Key=CITY_CENTERS_KEY)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def build_city_lookup(cities: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """city_norm / ascii / alternate name → best (highest population) coords."""
    lookup: dict[str, dict[str, Any]] = {}

    def put(key: str, payload: dict[str, Any]) -> None:
        if not key:
            return
        prev = lookup.get(key)
        if prev is None or payload["population"] > prev["population"]:
            lookup[key] = payload

    for row in cities.itertuples(index=False):
        payload = {
            "name": row.name,
            "lat": float(row.lat),
            "lon": float(row.lon),
            "population": int(getattr(row, "population", 0) or 0),
        }
        put(_norm_name(getattr(row, "city_norm", "") or row.name), payload)
        put(_norm_name(getattr(row, "asciiname", "")), payload)
        alts = getattr(row, "alternatenames", "") or ""
        if alts and isinstance(alts, str):
            for alt in alts.split(","):
                put(_norm_name(alt), payload)
    return lookup


def _ch_query_df(sql: str, *, timeout: int = 180) -> pd.DataFrame:
    text = ch_execute(sql, timeout=timeout, database=CH_DATABASE_HELPDESK)
    if not text.strip():
        return pd.DataFrame()
    if re.search(r"FORMAT\s+JSONEachRow\b", sql, flags=re.IGNORECASE):
        return pd.read_json(io.StringIO(text), lines=True)
    return pd.read_csv(io.StringIO(text), sep="\t")


def load_watermark() -> str | None:
    """Read incremental cursor from Airflow Variable. Empty / epoch → backfill."""
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


def clear_watermark() -> None:
    """Reset cursor so the next run does a lookback backfill."""
    from airflow.models import Variable

    Variable.set(WATERMARK_VAR, "")
    logger.info("watermark cleared %s", WATERMARK_VAR)


def fetch_tickets_delta(
    *,
    lookback_days: int = 90,
    watermark_updated_at: str | None = None,
    overlap_minutes: int = DEFAULT_OVERLAP_MINUTES,
    full_refresh: bool = False,
) -> tuple[pd.DataFrame, str]:
    """Return (tickets_df, mode) where mode is backfill|incremental.

    Address source (temporary): entity.legal_address via cur_entities.
    Places / place_id are not used.
    """
    if full_refresh or not watermark_updated_at:
        mode = "backfill"
        where = (
            f"t.created_at >= now() - INTERVAL {int(lookback_days)} DAY "
            "AND e.legal_address IS NOT NULL "
            "AND trimBoth(e.legal_address) != ''"
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
            f"t.updated_at >= parseDateTime64BestEffort({_sql_str(lower)}, 3) "
            "AND e.legal_address IS NOT NULL "
            "AND trimBoth(e.legal_address) != ''"
        )

    sql = f"""
SELECT
  toString(t.id) AS ticket_id,
  t.number AS ticket_number,
  t.created_at AS created_at,
  t.updated_at AS updated_at,
  t.current_status_name AS status_name,
  t.service_theme AS service_theme,
  t.user_group_name AS user_group_name,
  t.new_channel AS channel_name,
  toString(t.entity_id) AS entity_id,
  replaceRegexpAll(
    coalesce(nullIf(trimBoth(e.legal_address), ''), ''),
    '[\\\\x00-\\\\x1F]+',
    ' '
  ) AS legal_address
FROM mart_tickets_enriched AS t
ANY LEFT JOIN cur_entities AS e FINAL ON t.entity_id = e.id
WHERE {where}
FORMAT JSONEachRow
"""
    return _ch_query_df(sql, timeout=300), mode


def extract_city(place_city: Any, place_address: Any) -> str:
    """Normalize city from optional city field or parse Russian address text.

    Important: empty CH fields often arrive as float NaN via TabSeparated;
    str(nan) == 'nan' must NOT short-circuit address parsing.
    """
    city_text = ""
    if place_city is not None:
        try:
            if not pd.isna(place_city):
                city_text = str(place_city).strip()
        except (TypeError, ValueError):
            city_text = str(place_city).strip()
    if city_text.lower() not in _BAD_CITY_TOKENS:
        return _norm_name(city_text)

    address = str(place_address or "").strip()
    if not address or address.lower() in _BAD_CITY_TOKENS:
        return ""

    for pattern in (_CITY_PREFIX_RE, _CITY_COMPACT_RE):
        match = pattern.search(address)
        if match:
            candidate = _norm_name(match.group(1))
            if (
                candidate
                and candidate not in _BAD_CITY_TOKENS
                and not _SKIP_TOKEN_RE.match(candidate)
                and len(candidate) >= 3
            ):
                return candidate

    # Fallback: first comma-separated token without digits (rare free-text city).
    first = address.split(",")[0].strip() if address else ""
    if 2 <= len(first) <= 40 and not re.search(r"\d", first):
        candidate = _norm_name(first)
        if candidate not in _BAD_CITY_TOKENS and not _SKIP_TOKEN_RE.match(candidate):
            return candidate
    return ""


def resolve_city_key(
    city_field: Any,
    address: Any,
    lookup: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any] | None]:
    """Pick best city_norm + lookup hit from city field and/or address text."""
    raw_candidates: list[str] = []

    city_text = ""
    if city_field is not None:
        try:
            if not pd.isna(city_field):
                city_text = str(city_field).strip()
        except (TypeError, ValueError):
            city_text = str(city_field).strip()
    if city_text.lower() not in _BAD_CITY_TOKENS:
        raw_candidates.append(city_text)

    parsed = extract_city(None, address)
    if parsed:
        raw_candidates.append(parsed)

    addr = str(address or "")
    for raw in re.split(r"[,;]", addr):
        token = raw.strip()
        if not token:
            continue
        token = re.sub(r"^\d+\s*[-.]?\s*", "", token).strip()
        if not token:
            continue
        if re.search(r"(?i)\u0443\u043b\.|\u0443\u043b\u0438\u0446\u0430|\u043f\u0440-\u043a\u0442|\u043f\u0440\u043e\u0441\u043f\u0435\u043a\u0442|\b\u043f\u0435\u0440\.|\b\u0434\.\s*\d", token) and not re.search(
            r"(?i)(?:\u0433\.|\u0433\u043e\u0440\u043e\u0434|\b\u0433\s)", token
        ):
            # Street-only token unless it still contains a city marker.
            words = [w for w in re.split(r"[\s/]+", token) if w]
            for i, word in enumerate(words):
                raw_candidates.append(word)
                if i + 1 < len(words):
                    raw_candidates.append(f"{word} {words[i + 1]}")
            continue
        if len(token) > 80:
            words = [w for w in re.split(r"[\s/]+", token) if w]
            for i, word in enumerate(words):
                raw_candidates.append(word)
                if i + 1 < len(words):
                    raw_candidates.append(f"{word} {words[i + 1]}")
                    raw_candidates.append(f"{word}-{words[i + 1]}")
            continue
        raw_candidates.append(token)
        words = [w for w in re.split(r"[\s/]+", token) if w]
        for i, word in enumerate(words):
            raw_candidates.append(word)
            if i + 1 < len(words):
                raw_candidates.append(f"{word} {words[i + 1]}")
                raw_candidates.append(f"{word}-{words[i + 1]}")

    keys: list[str] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        for key in _candidate_keys(raw):
            if key not in seen:
                seen.add(key)
                keys.append(key)

    best_key = ""
    best_hit: dict[str, Any] | None = None
    for key in keys:
        hit = lookup.get(key)
        if hit is None:
            continue
        if best_hit is None or hit["population"] > best_hit["population"]:
            best_key = key
            best_hit = hit
    if best_hit is not None:
        return best_key, best_hit
    for key in keys:
        if key:
            return key, None
    return "", None


def geocode_tickets(tickets: pd.DataFrame, lookup: dict[str, dict[str, Any]]) -> pd.DataFrame:
    if tickets.empty:
        return tickets.copy()

    out = tickets.copy()
    # Prefer entity legal_address; fall back to legacy place_* if present.
    if "legal_address" in out.columns:
        addr_series = out["legal_address"]
        city_series = pd.Series([""] * len(out), index=out.index)
    else:
        city_series = out.get("place_city", pd.Series([""] * len(out), index=out.index))
        addr_series = out.get("place_address", pd.Series([""] * len(out), index=out.index))

    keys: list[str] = []
    resolved_name: list[str] = []
    lats: list[float | None] = []
    lons: list[float | None] = []
    matched: list[bool] = []
    for city, addr in zip(city_series.tolist(), addr_series.tolist()):
        key, hit = resolve_city_key(city, addr, lookup)
        keys.append(key)
        if hit:
            resolved_name.append(hit["name"])
            lats.append(hit["lat"])
            lons.append(hit["lon"])
            matched.append(True)
        else:
            resolved_name.append("")
            lats.append(None)
            lons.append(None)
            matched.append(False)

    out["city_norm"] = keys
    out["city_resolved"] = resolved_name
    out["lat"] = lats
    out["lon"] = lons
    out["geo_matched"] = matched
    out["geo_precision"] = ["city" if ok else "none" for ok in matched]
    out["geo_source"] = ["entity_legal_address" if ok else "" for ok in matched]
    return out


def ensure_target_table() -> None:
    ddl = f"""
CREATE TABLE IF NOT EXISTS {CH_DATABASE_HELPDESK}.{TARGET_TABLE}
(
  ticket_id UUID,
  ticket_number Int32,
  created_at DateTime64(3),
  updated_at DateTime64(3),
  status_name LowCardinality(String),
  service_theme LowCardinality(String),
  user_group_name LowCardinality(String),
  channel_name LowCardinality(String),
  place_id Nullable(UUID),
  place_region String,
  place_city String,
  place_address String,
  city_norm String,
  city_resolved String,
  lat Nullable(Float64),
  lon Nullable(Float64),
  geo_matched Bool,
  geo_precision LowCardinality(String),
  geo_source LowCardinality(String),
  built_at DateTime64(3)
)
ENGINE = ReplicatedReplacingMergeTree(built_at)
ORDER BY (ticket_id)
PARTITION BY toYYYYMM(created_at)
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


def insert_ticket_geo(df: pd.DataFrame) -> int:
    """Append/replace rows (ReplacingMergeTree keeps latest built_at per ticket_id)."""
    ensure_target_table()
    if df.empty:
        return 0

    def col(name: str, default: Any = "") -> pd.Series:
        if name in df.columns:
            return df[name]
        return pd.Series([default] * len(df), index=df.index)

    def _ch_dt(value: Any) -> str | None:
        """ClickHouse DateTime64 JSONEachRow rejects '+00:00' / 'NaT'."""
        if value is None or pd.isna(value):
            return None
        ts = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts.tz_convert(None).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _ch_float(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        if num != num or num in (float("inf"), float("-inf")):  # NaN / Inf
            return None
        return num

    def _ch_uuid(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "nat", "<na>", "null", "\\n"}:
            return None
        try:
            return str(uuid.UUID(text))
        except (ValueError, AttributeError, TypeError):
            return None

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

    def _jsonable(value: Any) -> Any:
        """Scrub non-JSON values before dumps."""
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
        if isinstance(value, str):
            return value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        return value

    built = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    n = len(df)
    records: list[dict[str, Any]] = []
    status = [_ch_str(v) for v in col("status_name").tolist()]
    theme = [_ch_str(v) for v in col("service_theme").tolist()]
    group = [_ch_str(v) for v in col("user_group_name").tolist()]
    channel = [_ch_str(v) for v in col("channel_name").tolist()]
    # CH table still has place_* columns; store legal_address there for now.
    if "legal_address" in df.columns:
        address = [_ch_str(v) for v in df["legal_address"].tolist()]
    else:
        address = [_ch_str(v) for v in col("place_address").tolist()]
    city_norm = [_ch_str(v) for v in col("city_norm").tolist()]
    city_resolved = [_ch_str(v) for v in col("city_resolved").tolist()]
    precision = [_ch_str(v) or "none" for v in col("geo_precision", "none").tolist()]
    source = [_ch_str(v) for v in col("geo_source").tolist()]
    ticket_ids = [_ch_uuid(v) for v in df["ticket_id"].tolist()]
    ticket_numbers = pd.to_numeric(df["ticket_number"], errors="coerce").fillna(0).astype(int).tolist()
    created = [_ch_dt(v) for v in df["created_at"].tolist()]
    updated = [_ch_dt(v) for v in df["updated_at"].tolist()]
    lats = [_ch_float(v) for v in col("lat", None).tolist()]
    lons = [_ch_float(v) for v in col("lon", None).tolist()]
    matched = [
        bool(v) if not (v is None or (isinstance(v, float) and pd.isna(v))) else False
        for v in col("geo_matched", False).tolist()
    ]

    dropped = 0
    for i in range(n):
        if not ticket_ids[i] or not created[i] or not updated[i]:
            dropped += 1
            continue
        records.append(
            {
                "ticket_id": ticket_ids[i],
                "ticket_number": ticket_numbers[i],
                "created_at": created[i],
                "updated_at": updated[i],
                "status_name": status[i],
                "service_theme": theme[i],
                "user_group_name": group[i],
                "channel_name": channel[i],
                "place_id": None,
                "place_region": "",
                "place_city": "",
                "place_address": address[i],
                "city_norm": city_norm[i],
                "city_resolved": city_resolved[i],
                "lat": lats[i],
                "lon": lons[i],
                "geo_matched": bool(matched[i]),
                "geo_precision": precision[i],
                "geo_source": source[i] or "entity_legal_address",
                "built_at": built,
            }
        )
    if dropped:
        logger.warning("dropped %s rows with invalid ticket_id/created_at/updated_at", dropped)
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


def run_ticket_geo(
    *,
    lookback_days: int = 90,
    force_ref_refresh: bool = False,
    force_full_refresh: bool = False,
    overlap_minutes: int = DEFAULT_OVERLAP_MINUTES,
) -> dict[str, Any]:
    ref = ensure_city_centers_on_s3(force=force_ref_refresh)
    cities = load_city_centers()
    lookup = build_city_lookup(cities)

    wm = None if force_full_refresh else load_watermark()
    watermark_before = wm

    tickets, mode = fetch_tickets_delta(
        lookback_days=lookback_days,
        watermark_updated_at=watermark_before,
        overlap_minutes=overlap_minutes,
        full_refresh=force_full_refresh,
    )
    geo = geocode_tickets(tickets, lookup)
    inserted = insert_ticket_geo(geo)

    matched = int(geo["geo_matched"].sum()) if not geo.empty else 0
    watermark_after = watermark_before

    if not geo.empty:
        max_updated = pd.to_datetime(geo["updated_at"], errors="coerce").max()
        if pd.notna(max_updated):
            watermark_after = max_updated.strftime("%Y-%m-%d %H:%M:%S")
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
        "tickets": int(len(geo)),
        "matched": matched,
        "match_rate": (matched / len(geo)) if len(geo) else 0.0,
        "inserted": inserted,
        "watermark_var": WATERMARK_VAR,
        "watermark_before": watermark_before,
        "watermark_after": watermark_after,
        "table": f"{CH_DATABASE_HELPDESK}.{TARGET_TABLE}",
        "view": f"{CH_DATABASE_HELPDESK}.{TARGET_VIEW}",
    }
