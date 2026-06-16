"""CRM OAuth token refresh — new DAG in dwh/ (legacy crm/ not touched)."""

from __future__ import annotations

import json
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="dwh_refresh_crm_token",
    schedule="*/90 * * * *",
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "crm", "token"],
)
def dwh_refresh_crm_token():
    @task
    def refresh() -> str:
        payload = {
            "client_id": Variable.get("crm_client_id"),
            "client_secret": Variable.get("crm_client_secret"),
            "grant_type": "refresh_token",
            "refresh_token": Variable.get("crm_refresh_token"),
            "redirect_uri": Variable.get("crm_redirect_uri", default_var="https://example.com"),
        }
        resp = requests.post("https://aoesp.crm.ru/oauth2/access_token", data=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        Variable.set("crm_access_token", data["access_token"])
        Variable.set("crm_refresh_token", data["refresh_token"])
        return data["access_token"]

    refresh()


dwh_refresh_crm_token()
