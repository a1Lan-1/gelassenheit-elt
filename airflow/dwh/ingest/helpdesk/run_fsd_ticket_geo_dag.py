"""Daily FSD ticket geo for BI maps (incremental pandas + S3 city centers)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.sdk import dag, task

from dwh.common.schedules import FSD_TICKET_GEO_CRON_MSK, resolve_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "fsd_ticket_geo.yaml"


def _cfg() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _schedule() -> object:
    return resolve_schedule(_cfg().get("schedule", FSD_TICKET_GEO_CRON_MSK))


@dag(
    dag_id="dwh_run_fsd_ticket_geo",
    schedule=_schedule(),
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "helpdesk", "geo", "BI"],
    max_active_runs=1,
)
def run_fsd_ticket_geo():
    @task
    def build_ticket_geo() -> dict:
        from dwh.common.ticket_geo import run_ticket_geo

        cfg = _cfg()
        return run_ticket_geo(
            lookback_days=int(cfg.get("lookback_days", 90)),
            force_ref_refresh=bool(cfg.get("force_ref_refresh", False)),
            force_full_refresh=bool(cfg.get("force_full_refresh", False)),
            overlap_minutes=int(cfg.get("overlap_minutes", 30)),
        )

    build_ticket_geo()


run_fsd_ticket_geo()
