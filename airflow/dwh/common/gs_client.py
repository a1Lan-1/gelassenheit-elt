"""Google Sheets client from Airflow Connection (service account JSON in extra)."""

from __future__ import annotations

import json
import logging

import gspread
from airflow.hooks.base import BaseHook
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _service_account_info(conn_id: str) -> dict:
    conn = BaseHook.get_connection(conn_id)
    info = conn.extra_dejson or {}
    if not info and conn.extra:
        info = json.loads(conn.extra)
    if not info.get("private_key"):
        raise ValueError(
            f"Connection {conn_id!r} has no service account JSON in extra "
            "(expected type, client_email, private_key, ...)"
        )
    return info


def gspread_client(conn_id: str) -> gspread.Client:
    creds = Credentials.from_service_account_info(_service_account_info(conn_id), scopes=SCOPES)
    return gspread.authorize(creds)


def read_range(
    *,
    conn_id: str,
    spreadsheet_id: str,
    range_name: str,
    has_header: bool,
    columns: list[str],
    value_render_option: str | None = None,
    date_time_render_option: str | None = None,
    short_row_pad: str = "left",
    validate_headers: bool = False,
) -> list[list[object]]:
    client = gspread_client(conn_id)
    sheet = client.open_by_key(spreadsheet_id)
    if "!" in range_name:
        sheet_name, cell_range = range_name.split("!", 1)
    else:
        sheet_name, cell_range = range_name, ""
    worksheet = sheet.worksheet(sheet_name)
    get_kwargs: dict[str, object] = {}
    if value_render_option:
        get_kwargs["value_render_option"] = value_render_option
    if date_time_render_option:
        get_kwargs["date_time_render_option"] = date_time_render_option
    data = worksheet.get(cell_range, **get_kwargs) if cell_range else worksheet.get_all_values(**get_kwargs)
    if not data:
        raise ValueError(f"No data in Google Sheets range {range_name!r}")

    if has_header:
        headers = [str(h).strip() for h in data[0]]
        rows = data[1:]
        expected = len(columns)
        if len(headers) != expected:
            raise ValueError(
                f"Google Sheets {range_name!r}: header count {len(headers)} != "
                f"config columns {expected} ({columns})"
            )
        if validate_headers:
# Mapping is always by position; when validate_headers, names must match columns.
            norm_headers = [h.lower() for h in headers]
            norm_columns = [c.lower() for c in columns]
            if norm_headers != norm_columns:
                raise ValueError(
                    f"Google Sheets {range_name!r}: header names {headers} != "
                    f"config columns {columns}"
                )
        width = expected
    else:
        headers = columns
        rows = data
        width = len(columns)
    padded: list[list[object]] = []
    pad_side = (short_row_pad or "left").lower()
    if pad_side not in ("left", "right"):
        raise ValueError(f"short_row_pad must be 'left' or 'right', got {short_row_pad!r}")
    for row in rows:
        cells = list(row)
        if len(cells) < width:
            missing = width - len(cells)
            if pad_side == "right":
                # Trailing empty cells (phone, birthday, etc.) are omitted by the API.
                cells = cells + [""] * missing
            else:
                # Legacy schedule rows may omit leading empty cells (column A = dt).
                cells = [""] * missing + cells
        padded.append(cells[:width])
    logger.info("Read %s rows from %s (%s)", len(padded), spreadsheet_id, range_name)
    return padded
