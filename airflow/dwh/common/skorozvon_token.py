"""Dialer OAuth token refresh (legacy dialer_key_update parity)."""

from __future__ import annotations

import json
import logging

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)

TOKEN_URL = "https://api.dialer.ru/oauth/token"


def _password_grant() -> dict:
    return {
        "grant_type": (None, "password"),
        "username": (None, Variable.get("dialer_username")),
        "api_key": (None, Variable.get("dialer_api_key")),
        "client_id": (None, Variable.get("dialer_client_id")),
        "client_secret": (None, Variable.get("dialer_app_key")),
    }


def _refresh_grant(access_token: str, refresh_token: str) -> tuple[dict, dict]:
    files = {
        "grant_type": (None, "refresh_token"),
        "refresh_token": (None, refresh_token),
        "client_id": (None, Variable.get("dialer_client_id")),
        "client_secret": (None, Variable.get("dialer_app_key")),
    }
    headers = {"Authorization": f"Bearer {access_token}"}
    return files, headers


def _save_tokens(data: dict) -> None:
    access = data.get("access_token")
    refresh = data.get("refresh_token")
    if not access or not refresh:
        raise RuntimeError(f"Dialer token response missing tokens: keys={list(data.keys())}")
    Variable.set("dialer_access_token", access)
    Variable.set("dialer_refresh_token", refresh)


def refresh_dialer_tokens() -> str:
    access = Variable.get("dialer_access_token", default_var=None)
    refresh = Variable.get("dialer_refresh_token", default_var=None)

    if not access or not refresh:
        logger.info("Dialer tokens missing; password grant")
        response = requests.post(TOKEN_URL, files=_password_grant(), timeout=60)
    else:
        logger.info("Dialer refresh_token grant")
        files, headers = _refresh_grant(access, refresh)
        response = requests.post(TOKEN_URL, files=files, headers=headers, timeout=60)

    if response.status_code == 200:
        data = response.json()
        _save_tokens(data)
        logger.info("Dialer tokens updated")
        return data["access_token"]

    if refresh and response.status_code in (400, 401):
        logger.warning("Dialer refresh failed (%s); password grant", response.status_code)
        response = requests.post(TOKEN_URL, files=_password_grant(), timeout=60)
        if response.status_code == 200:
            data = response.json()
            _save_tokens(data)
            return data["access_token"]

    try:
        detail = response.json()
    except json.JSONDecodeError:
        detail = response.text[:500]
    raise RuntimeError(f"Dialer token request failed: {response.status_code} {detail}")
