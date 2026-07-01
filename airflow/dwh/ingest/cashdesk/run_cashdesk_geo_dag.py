"""Daily cashdesk geo dictionary (address → city lat/lon)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.sdk import dag, task

from dwh.common.schedules import CASHDESK_GEO_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "cashdesk_geo.yaml"


def _cfg() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _schedule() -> object:
    return resolve_schedule(_cfg().get("schedule", CASHDESK_GEO_CRON_MSK))


@dag(
    dag_id="dwh_run_cashdesk_geo",
    schedule=_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "cashdesk", "geo", "BI"],
    max_active_runs=1,
)
def run_cashdesk_geo_dag():
    @task
    def build_cashdesk_geo() -> dict:
        from dwh.common.cashdesk_geo import run_cashdesk_geo

        cfg = _cfg()
        return run_cashdesk_geo(
            force_ref_refresh=bool(cfg.get("force_ref_refresh", False)),
            force_full_refresh=bool(cfg.get("force_full_refresh", False)),
            overlap_minutes=int(cfg.get("overlap_minutes", 30)),
        )

    build_cashdesk_geo()


run_cashdesk_geo_dag()
