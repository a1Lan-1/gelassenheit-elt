"""Nextcloud WebDAV client (Basic Auth from Airflow Connection)."""

from __future__ import annotations

import logging
from urllib.parse import quote

import requests
from airflow.sdk.bases.hook import BaseHook

logger = logging.getLogger(__name__)


def _encode_dav_path(path: str) -> str:
    """Encode path segments, keep slashes."""
    path = path if path.startswith("/") else f"/{path}"
    parts = path.split("/")
    return "/".join(quote(p, safe="") if p else "" for p in parts)


def _build_url(origin: str, dav_path: str, login: str) -> str:
    """Build URL. Leave full /remote.php/... unchanged; prefix relative paths with login/UUID."""
    path = dav_path.strip()
    if not path.startswith("/remote.php/"):
# relative root path files/<login or uuid>/
        path = f"/remote.php/dav/files/{login}/{path.lstrip('/')}"
    return f"{origin}{_encode_dav_path(path)}"


def download_dav_file(*, conn_id: str, host: str, dav_path: str, timeout: int = 120) -> bytes:
    """GET xlsx (or other file) by WebDAV path."""
    conn = BaseHook.get_connection(conn_id)
    login = (conn.login or "").strip()
    password = conn.password or ""
    extra = conn.extra_dejson or {}
    # On this Nextcloud WebDAV root = User UUID, not login.
    dav_user = (extra.get("dav_user") or login).strip()
    if not login or not password:
        raise ValueError(f"Connection {conn_id!r}: login/password required")

    base = (conn.host or host or "").rstrip("/")
    if base.startswith("http://") or base.startswith("https://"):
        origin = base
    else:
        schema = (conn.schema or "https").rstrip(":/")
        if schema not in ("http", "https"):
            schema = "https"
        origin = f"{schema}://{base}"

    # Full path from yaml / extra.dav_path takes precedence.
    path = (extra.get("dav_path") or dav_path).strip()
    url = _build_url(origin, path, dav_user)

    logger.info(
        "Nextcloud GET %s (conn=%s login=%s dav_user=%s)",
        "/".join(url.split("/")[-3:])[:160],
        conn_id,
        login,
        dav_user,
    )
    resp = requests.get(
        url,
        auth=(login, password),
        headers={
            "User-Agent": "gelassenheit-airflow-nc/1.0",
            "OCS-APIRequest": "true",
        },
        timeout=timeout,
    )
    if resp.status_code in (401, 403):
        body = (resp.text or "")[:300].replace("\n", " ")
        raise PermissionError(
            f"Nextcloud {resp.status_code} for {url}. "
            f"Check login/password and extra.dav_user (UUID) on Connection {conn_id!r}. "
            f"body={body!r}"
        )
    resp.raise_for_status()
    data = resp.content
    if not data:
        raise ValueError(f"Empty response from Nextcloud: {url}")
    logger.info("Nextcloud downloaded %s bytes", len(data))
    return data
