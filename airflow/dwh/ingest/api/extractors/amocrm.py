"""CRM API extractors for bronze ingest."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

try:
    from airflow.sdk import Variable
except ImportError:  # Airflow 2.x
    from airflow.models import Variable

from dwh.common.api_ingest import load_api_watermark
from dwh.common.watermark import exclusive_start_ts

logger = logging.getLogger(__name__)

AMOCRM_BASE_URL = "https://aoesp.crm.ru/api/v4"
PIPELINES = ("10003690", "10636682")
EVENTS_LIMIT = 100
EVENTS_WITH = "lead_name,contact_name,company_name,customer_name,catalog_element_name,catalog_name"
_API_EMPTY_RETRIES = 3
_API_EMPTY_SLEEP_SEC = 2.0


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {Variable.get('crm_access_token')}"}


def _parse_json_response(resp: requests.Response, *, path: str) -> dict:
    """Parse JSON; empty/HTML body raises a clear error (not JSONDecodeError on blank input)."""
    text = (resp.text or "").strip()
    if not text:
        raise RuntimeError(
            f"CRM empty response: GET {path} status={resp.status_code} "
            f"content-type={resp.headers.get('Content-Type')!r}"
        )
    try:
        payload = resp.json()
    except ValueError as exc:
        snippet = text[:300].replace("\n", " ")
        raise RuntimeError(
            f"CRM non-JSON response: GET {path} status={resp.status_code} "
            f"content-type={resp.headers.get('Content-Type')!r} body={snippet!r}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"CRM unexpected JSON type {type(payload).__name__}: GET {path}"
        )
    return payload


def _api_get(
    path: str,
    *,
    params: dict[str, Any] | list[tuple[str, Any]] | None = None,
) -> dict:
    """GET CRM API with short retries on empty body (gateway / rate-limit glitches)."""
    url = f"{AMOCRM_BASE_URL}{path}"
    last_exc: Exception | None = None
    for attempt in range(1, _API_EMPTY_RETRIES + 1):
        resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        resp.raise_for_status()
        try:
            return _parse_json_response(resp, path=path)
        except RuntimeError as exc:
            last_exc = exc
            if "empty response" not in str(exc) or attempt >= _API_EMPTY_RETRIES:
                raise
            logger.warning(
                "CRM empty body on %s (attempt %s/%s), sleep %.1fs",
                path,
                attempt,
                _API_EMPTY_RETRIES,
                _API_EMPTY_SLEEP_SEC,
            )
            time.sleep(_API_EMPTY_SLEEP_SEC)
    assert last_exc is not None
    raise last_exc


def _field_value(custom_fields: list[dict] | None, field_name: str, field_id: int | None = None) -> Any:
    if not custom_fields:
        return None
    for field in custom_fields:
        if field.get("field_name") == field_name or (field_id is not None and field.get("field_id") == field_id):
            values = field.get("values") or []
            return values[0].get("value") if values else None
    return None


def _ts(value: Any) -> datetime | None:
    return datetime.fromtimestamp(value, tz=timezone.utc) if isinstance(value, (int, float)) else None


def _dump_embedded_list(embedded: dict | None, key: str) -> str:
    """Serialize an `_embedded` list field; missing key becomes empty list JSON."""
    embedded = embedded or {}
    return json.dumps(embedded.get(key) or [], ensure_ascii=False)


def _tags_json_is_empty(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    if text in ("", "[]", "null", "None", "{}", '{"items":[]}'):
        return True
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        return False
    if parsed in ([], {}):
        return True
    if isinstance(parsed, dict) and parsed.get("items") in ([], None):
# {"items":[]} after CH-wrapper / empty items
        return "items" in parsed
    return False


def _backfill_empty_entity_tags(
    *,
    path: str,
    embedded_key: str,
    rows: list[dict],
    id_key: str = "id",
    tags_key: str = "tags",
    chunk_size: int = 50,
    with_param: str | None = None,
) -> int:
    """Re-fetch entities whose list payload had empty tags.

    CRM list endpoints sometimes return `_embedded.tags: []` even when the
    entity still has tags; a filtered GET by id returns the real list. Without
    this, ReplacingMergeTree / ON CONFLICT silently overwrite good tags.
    """
    empty_ids = [
        row[id_key]
        for row in rows
        if row.get(id_key) is not None and _tags_json_is_empty(row.get(tags_key))
    ]
    if not empty_ids:
        return 0

    recovered = 0
    by_id = {row[id_key]: row for row in rows}
    for offset in range(0, len(empty_ids), chunk_size):
        chunk = empty_ids[offset : offset + chunk_size]
        params: list[tuple[str, Any]] = [("limit", min(250, len(chunk)))]
        if with_param:
            params.append(("with", with_param))
        params.extend(("filter[id][]", entity_id) for entity_id in chunk)
        payload = _api_get(path, params=params)
        entities = payload.get("_embedded", {}).get(embedded_key, [])
        for entity in entities:
            entity_id = entity.get("id")
            row = by_id.get(entity_id)
            if row is None:
                continue
            tags = (entity.get("_embedded") or {}).get("tags") or []
            if not tags:
                continue
            row[tags_key] = json.dumps(tags, ensure_ascii=False)
            recovered += 1
        time.sleep(0.3)
    return recovered


def extract_deals() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "deals"))

    rows: list[dict] = []
    for pipeline in PIPELINES:
        page = 1
        while True:
            payload = _api_get(
                "/leads",
                params={
                    "limit": 250,
                    "page": page,
                    "filter[pipeline_id]": pipeline,
                    "filter[updated_at][from]": cutoff_ts,
                    "with": "contacts,companies,catalog_elements,loss_reason",
                },
            )
            leads = payload.get("_embedded", {}).get("leads", [])
            if not leads:
                break
            for lead in leads:
                custom_fields = lead.get("custom_fields_values")
                embedded = lead.get("_embedded", {})
                companies = embedded.get("companies") or []
                raw_row = {
                    "id": lead.get("id"),
                    "name": lead.get("name"),
                    "price": lead.get("price"),
                    "responsible_user_id": lead.get("responsible_user_id"),
                    "group_id": lead.get("group_id"),
                    "status_id": lead.get("status_id"),
                    "pipeline_id": lead.get("pipeline_id"),
                    "loss_reason_id": lead.get("loss_reason_id"),
                    "source_id": lead.get("source_id"),
                    "created_by": lead.get("created_by"),
                    "updated_by": lead.get("updated_by"),
                    "closed_at": _ts(lead.get("closed_at")),
                    "created_at": _ts(lead.get("created_at")),
                    "updated_at": _ts(lead.get("updated_at")),
                    "closest_task_at": _ts(lead.get("closest_task_at")),
                    "is_deleted": lead.get("is_deleted"),
                    "custom_fields_values": json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
                    "score": lead.get("score"),
                    "account_id": lead.get("account_id"),
                    "labor_cost": lead.get("labor_cost"),
                    "is_price_modified_by_robot": lead.get("is_price_modified_by_robot"),
                    "tags": _dump_embedded_list(embedded, "tags"),
                    "contacts": _dump_embedded_list(embedded, "contacts"),
                    "company_id": companies[0].get("id") if companies else None,
                    "msb_deal_completed": _ts(_field_value(custom_fields, "MSB deal completed")),
                    "msb_stage_date": _ts(_field_value(custom_fields, "MSB stage date")),
                    "msb_contact_date": _ts(_field_value(custom_fields, "MSB contact date")),
                    "msb_first_contact_date": _ts(_field_value(custom_fields, "MSB first contact date")),
                    "msb_last_call_date": _ts(_field_value(custom_fields, "MSB last call date (decision maker)")),
                    "msb_contact_form": _field_value(custom_fields, "MSB primary contact form"),
                    "kkt_count": _field_value(custom_fields, "KKT count by slice"),
                }
                rows.append(raw_row)
            if len(leads) < 250:
                break
            page += 1
            time.sleep(0.3)

    if not rows:
        return pd.DataFrame()
    recovered = _backfill_empty_entity_tags(
        path="/leads",
        embedded_key="leads",
        rows=rows,
        with_param="contacts,companies,catalog_elements,loss_reason",
    )
    if recovered:
        logger.info("crm deals: recovered tags for %s leads via id re-fetch", recovered)
    return pd.DataFrame(rows)


def extract_companies() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "companies"))

    rows: list[dict] = []
    page = 1
    while True:
        payload = _api_get(
            "/companies",
            params={
                "limit": 250,
                "page": page,
                "filter[updated_at][from]": cutoff_ts,
                "with": "contacts,customers,leads,catalog_elements",
            },
        )
        companies = payload.get("_embedded", {}).get("companies", [])
        if not companies:
            break
        for company in companies:
            custom_fields = company.get("custom_fields_values")
            embedded = company.get("_embedded", {})
            rows.append(
                {
                    "id": company.get("id"),
                    "name": company.get("name"),
                    "responsible_user_id": company.get("responsible_user_id"),
                    "group_id": company.get("group_id"),
                    "created_by": company.get("created_by"),
                    "updated_by": company.get("updated_by"),
                    "created_at": _ts(company.get("created_at")),
                    "updated_at": _ts(company.get("updated_at")),
                    "closest_task_at": _ts(company.get("closest_task_at")),
                    "is_deleted": company.get("is_deleted"),
                    "custom_fields_values": json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
                    "inn": _field_value(custom_fields, "Tax ID", 1068215),
                    "account_id": company.get("account_id"),
                    "tags": _dump_embedded_list(embedded, "tags"),
                    "customers": _dump_embedded_list(embedded, "customers"),
                    "leads": _dump_embedded_list(embedded, "leads"),
                    "catalog_elements": _dump_embedded_list(embedded, "catalog_elements"),
                }
            )
        if len(companies) < 250:
            break
        page += 1
        time.sleep(0.3)
    if not rows:
        return pd.DataFrame()
    recovered = _backfill_empty_entity_tags(path="/companies", embedded_key="companies", rows=rows)
    if recovered:
        logger.info("crm companies: recovered tags for %s companies via id re-fetch", recovered)
    return pd.DataFrame(rows)


def extract_users() -> pd.DataFrame:
    rows: list[dict] = []
    page = 1
    while True:
        payload = _api_get("/users", params={"limit": 250, "page": page})
        users = payload.get("_embedded", {}).get("users", [])
        if not users:
            break
        rows.extend({"id": user.get("id"), "name": user.get("name"), "email": user.get("email")} for user in users)
        if len(users) < 250:
            break
        page += 1
        time.sleep(0.3)
    return pd.DataFrame(rows)


def _entity_name(embedded: dict | None, entity_type: str | None) -> str | None:
    if not embedded:
        return None
    entity = embedded.get("entity")
    if isinstance(entity, dict):
        name = entity.get("name")
        if name:
            return str(name)
    for key in (
        "lead_name",
        "contact_name",
        "company_name",
        "customer_name",
        "catalog_element_name",
    ):
        value = embedded.get(key)
        if value:
            return str(value)
    return None


def extract_events() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "events"))

    rows: list[dict] = []
    page = 1
    while True:
        payload = _api_get(
            "/events",
            params={
                "limit": EVENTS_LIMIT,
                "page": page,
                "with": EVENTS_WITH,
                "filter[created_at][from]": cutoff_ts,
            },
        )
        events = payload.get("_embedded", {}).get("events", [])
        if not events:
            break
        for event in events:
            embedded = event.get("_embedded", {})
            rows.append(
                {
                    "id": event.get("id"),
                    "type": event.get("type"),
                    "entity_id": event.get("entity_id"),
                    "entity_type": event.get("entity_type"),
                    "created_by": event.get("created_by"),
                    "created_at": _ts(event.get("created_at")),
                    "value_after": json.dumps(event.get("value_after") or [], ensure_ascii=False),
                    "value_before": json.dumps(event.get("value_before") or [], ensure_ascii=False),
                    "account_id": event.get("account_id"),
                    "entity_name": _entity_name(embedded, event.get("entity_type")),
                    "embedded_entity": json.dumps(embedded, ensure_ascii=False) if embedded else None,
                }
            )
        if len(events) < EVENTS_LIMIT:
            break
        page += 1
        time.sleep(0.3)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def extract_deal_statuses() -> pd.DataFrame:
    rows: list[dict] = []
    for pipeline in PIPELINES:
        payload = _api_get(f"/leads/pipelines/{pipeline}/statuses", params={"limit": 250, "page": 1})
        statuses = payload.get("_embedded", {}).get("statuses", [])
        rows.extend(
            {
                "id": status.get("id"),
                "name": status.get("name"),
                "sort": status.get("sort"),
                "pipeline_id": status.get("pipeline_id"),
            }
            for status in statuses
        )
        time.sleep(0.3)
    return pd.DataFrame(rows)


def _paginate(path: str, embedded_key: str, *, params: dict[str, Any] | None = None, limit: int = 250) -> list[dict]:
    rows: list[dict] = []
    page = 1
    base_params = dict(params or {})
    while True:
        payload = _api_get(path, params={**base_params, "limit": limit, "page": page})
        batch = payload.get("_embedded", {}).get(embedded_key, [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        page += 1
        time.sleep(0.3)
    return rows


def _cf_phone_email(custom_fields: list[dict] | None) -> tuple[str | None, str | None]:
    phone = None
    email = None
    if not custom_fields:
        return phone, email
    for field in custom_fields:
        code = (field.get("field_code") or "").upper()
        values = field.get("values") or []
        if not values:
            continue
        val = values[0].get("value")
        if val is None:
            continue
        if code == "PHONE" and phone is None:
            phone = str(val)
        elif code == "EMAIL" and email is None:
            email = str(val)
    return phone, email


def extract_pipelines() -> pd.DataFrame:
    rows = []
    for pipe in _paginate("/leads/pipelines", "pipelines"):
        rows.append(
            {
                "id": pipe.get("id"),
                "name": pipe.get("name"),
                "sort": pipe.get("sort"),
                "is_main": pipe.get("is_main"),
                "is_archive": pipe.get("is_archive"),
                "account_id": pipe.get("account_id"),
                "raw_data": json.dumps(pipe, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_loss_reasons() -> pd.DataFrame:
    rows = []
    try:
        items = _paginate("/leads/loss_reasons", "loss_reasons")
    except requests.HTTPError:
        return pd.DataFrame()
    for item in items:
        rows.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "sort": item.get("sort"),
                "account_id": item.get("account_id"),
                "raw_data": json.dumps(item, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_roles() -> pd.DataFrame:
    rows = []
    try:
        items = _paginate("/roles", "roles")
    except requests.HTTPError:
        return pd.DataFrame()
    for item in items:
        rows.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "raw_data": json.dumps(item, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_sources() -> pd.DataFrame:
    rows = []
    try:
        items = _paginate("/sources", "sources")
    except requests.HTTPError:
        return pd.DataFrame()
    for item in items:
        rows.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "external_id": item.get("external_id"),
                "raw_data": json.dumps(item, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_tags() -> pd.DataFrame:
    rows = []
    for entity_type in ("leads", "contacts", "companies", "customers"):
        try:
            items = _paginate(f"/{entity_type}/tags", "tags")
        except requests.HTTPError:
            continue
        for item in items:
            rows.append(
                {
                    "id": item.get("id"),
                    "entity_type": entity_type,
                    "name": item.get("name"),
                    "color": item.get("color"),
                    "raw_data": json.dumps(item, ensure_ascii=False),
                }
            )
        time.sleep(0.2)
    return pd.DataFrame(rows)


def extract_custom_fields() -> pd.DataFrame:
    rows = []
    for entity_type in ("leads", "contacts", "companies", "customers"):
        try:
            items = _paginate(f"/{entity_type}/custom_fields", "custom_fields")
        except requests.HTTPError:
            continue
        for item in items:
            rows.append(
                {
                    "id": item.get("id"),
                    "entity_type": entity_type,
                    "name": item.get("name"),
                    "code": item.get("code"),
                    "type": item.get("type"),
                    "sort": item.get("sort"),
                    "is_api_only": item.get("is_api_only"),
                    "raw_data": json.dumps(item, ensure_ascii=False),
                }
            )
        time.sleep(0.2)
    # catalog custom fields
    for catalog in _paginate("/catalogs", "catalogs"):
        catalog_id = catalog.get("id")
        if not catalog_id:
            continue
        try:
            items = _paginate(f"/catalogs/{catalog_id}/custom_fields", "custom_fields")
        except requests.HTTPError:
            continue
        for item in items:
            rows.append(
                {
                    "id": item.get("id"),
                    "entity_type": f"catalogs:{catalog_id}",
                    "name": item.get("name"),
                    "code": item.get("code"),
                    "type": item.get("type"),
                    "sort": item.get("sort"),
                    "is_api_only": item.get("is_api_only"),
                    "raw_data": json.dumps(item, ensure_ascii=False),
                }
            )
        time.sleep(0.2)
    return pd.DataFrame(rows)


def extract_catalogs() -> pd.DataFrame:
    rows = []
    for item in _paginate("/catalogs", "catalogs"):
        rows.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "type": item.get("type"),
                "can_add_elements": item.get("can_add_elements"),
                "can_link_multiple": item.get("can_link_multiple"),
                "raw_data": json.dumps(item, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_contacts() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "contacts"))
    items = _paginate(
        "/contacts",
        "contacts",
        params={"filter[updated_at][from]": cutoff_ts, "with": "leads,companies"},
    )
    rows = []
    for contact in items:
        custom_fields = contact.get("custom_fields_values")
        phone, email = _cf_phone_email(custom_fields)
        embedded = contact.get("_embedded") or {}
        rows.append(
            {
                "id": contact.get("id"),
                "name": contact.get("name"),
                "first_name": contact.get("first_name"),
                "last_name": contact.get("last_name"),
                "responsible_user_id": contact.get("responsible_user_id"),
                "group_id": contact.get("group_id"),
                "created_by": contact.get("created_by"),
                "updated_by": contact.get("updated_by"),
                "created_at": _ts(contact.get("created_at")),
                "updated_at": _ts(contact.get("updated_at")),
                "is_deleted": contact.get("is_deleted"),
                "account_id": contact.get("account_id"),
                "phone": phone,
                "email": email,
                "custom_fields_values": json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
                "tags": _dump_embedded_list(embedded, "tags"),
                "leads": _dump_embedded_list(embedded, "leads"),
                "companies": _dump_embedded_list(embedded, "companies"),
                "raw_data": json.dumps(contact, ensure_ascii=False),
            }
        )
    if not rows:
        return pd.DataFrame()
    recovered = _backfill_empty_entity_tags(path="/contacts", embedded_key="contacts", rows=rows)
    if recovered:
        logger.info("crm contacts: recovered tags for %s contacts via id re-fetch", recovered)
    return pd.DataFrame(rows)


def extract_tasks() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "tasks"))
    items = _paginate("/tasks", "tasks", params={"filter[updated_at][from]": cutoff_ts})
    rows = []
    for task in items:
        result = task.get("result") or {}
        rows.append(
            {
                "id": task.get("id"),
                "created_by": task.get("created_by"),
                "updated_by": task.get("updated_by"),
                "created_at": _ts(task.get("created_at")),
                "updated_at": _ts(task.get("updated_at")),
                "responsible_user_id": task.get("responsible_user_id"),
                "group_id": task.get("group_id"),
                "entity_id": task.get("entity_id"),
                "entity_type": task.get("entity_type"),
                "is_completed": task.get("is_completed"),
                "task_type_id": task.get("task_type_id"),
                "text": task.get("text"),
                "duration": task.get("duration"),
                "complete_till": _ts(task.get("complete_till")),
                "account_id": task.get("account_id"),
                "result_text": result.get("text") if isinstance(result, dict) else None,
                "raw_data": json.dumps(task, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_notes() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "notes"))
    rows = []
    for entity_type in ("leads", "contacts", "companies", "customers"):
        try:
            items = _paginate(
                f"/{entity_type}/notes",
                "notes",
                params={"filter[updated_at][from]": cutoff_ts},
            )
        except requests.HTTPError:
            continue
        for note in items:
            params = note.get("params") or {}
            text = None
            if isinstance(params, dict):
                text = params.get("text") or params.get("uniq") or params.get("phone")
            rows.append(
                {
                    "id": note.get("id"),
                    "entity_id": note.get("entity_id"),
                    "entity_type": entity_type,
                    "note_type": note.get("note_type"),
                    "text": text,
                    "created_by": note.get("created_by"),
                    "updated_by": note.get("updated_by"),
                    "created_at": _ts(note.get("created_at")),
                    "updated_at": _ts(note.get("updated_at")),
                    "responsible_user_id": note.get("responsible_user_id"),
                    "group_id": note.get("group_id"),
                    "account_id": note.get("account_id"),
                    "params": json.dumps(params, ensure_ascii=False) if params else None,
                    "raw_data": json.dumps(note, ensure_ascii=False),
                }
            )
        time.sleep(0.2)
    return pd.DataFrame(rows)


def extract_unsorted() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "unsorted"))
    items = _paginate("/leads/unsorted", "unsorted", params={"filter[created_at][from]": cutoff_ts})
    rows = []
    for item in items:
        rows.append(
            {
                "uid": item.get("uid"),
                "source_uid": item.get("source_uid"),
                "category": item.get("category"),
                "pipeline_id": item.get("pipeline_id"),
                "created_at": _ts(item.get("created_at")),
                "account_id": item.get("account_id"),
                "raw_data": json.dumps(item, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows)


def extract_customers() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "customers"))
    try:
        items = _paginate(
            "/customers",
            "customers",
            params={
                "filter[updated_at][from]": cutoff_ts,
                "with": "contacts,companies,catalog_elements",
            },
        )
    except requests.HTTPError:
        return pd.DataFrame()
    rows = []
    for customer in items:
        custom_fields = customer.get("custom_fields_values")
        embedded = customer.get("_embedded") or {}
        rows.append(
            {
                "id": customer.get("id"),
                "name": customer.get("name"),
                "next_price": customer.get("next_price"),
                "next_date": _ts(customer.get("next_date")),
                "responsible_user_id": customer.get("responsible_user_id"),
                "status_id": customer.get("status_id"),
                "periodicity": customer.get("periodicity"),
                "created_by": customer.get("created_by"),
                "updated_by": customer.get("updated_by"),
                "created_at": _ts(customer.get("created_at")),
                "updated_at": _ts(customer.get("updated_at")),
                "account_id": customer.get("account_id"),
                "ltv": customer.get("ltv"),
                "purchases_count": customer.get("purchases_count"),
                "average_check": customer.get("average_check"),
                "custom_fields_values": json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
                "tags": _dump_embedded_list(embedded, "tags"),
                "raw_data": json.dumps(customer, ensure_ascii=False),
            }
        )
    if not rows:
        return pd.DataFrame()
    recovered = _backfill_empty_entity_tags(path="/customers", embedded_key="customers", rows=rows)
    if recovered:
        logger.info("crm customers: recovered tags for %s customers via id re-fetch", recovered)
    return pd.DataFrame(rows)


def extract_catalog_elements() -> pd.DataFrame:
    cutoff_ts = exclusive_start_ts(load_api_watermark("crm", "catalog_elements"))
    rows = []
    catalogs = _paginate("/catalogs", "catalogs")
    for catalog in catalogs:
        catalog_id = catalog.get("id")
        if not catalog_id:
            continue
        try:
            items = _paginate(
                f"/catalogs/{catalog_id}/elements",
                "elements",
                params={"filter[updated_at][from]": cutoff_ts},
            )
        except requests.HTTPError:
            continue
        for el in items:
            custom_fields = el.get("custom_fields_values")
            rows.append(
                {
                    "id": el.get("id"),
                    "catalog_id": catalog_id,
                    "name": el.get("name"),
                    "created_by": el.get("created_by"),
                    "updated_by": el.get("updated_by"),
                    "created_at": _ts(el.get("created_at")),
                    "updated_at": _ts(el.get("updated_at")),
                    "account_id": el.get("account_id"),
                    "custom_fields_values": json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
                    "raw_data": json.dumps(el, ensure_ascii=False),
                }
            )
        time.sleep(0.2)
    return pd.DataFrame(rows)


EXTRACTORS = {
    "deals": extract_deals,
    "companies": extract_companies,
    "users": extract_users,
    "deal_statuses": extract_deal_statuses,
    "events": extract_events,
    "pipelines": extract_pipelines,
    "loss_reasons": extract_loss_reasons,
    "roles": extract_roles,
    "sources": extract_sources,
    "tags": extract_tags,
    "custom_fields": extract_custom_fields,
    "catalogs": extract_catalogs,
    "contacts": extract_contacts,
    "tasks": extract_tasks,
    "notes": extract_notes,
    "unsorted": extract_unsorted,
    "customers": extract_customers,
    "catalog_elements": extract_catalog_elements,
}
