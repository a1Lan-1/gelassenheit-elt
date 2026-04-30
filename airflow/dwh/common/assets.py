"""Airflow 3 Assets for ClickHouse tables and DWH coordination gates."""

from __future__ import annotations

from pathlib import Path

import yaml
from airflow.sdk import Asset

from dwh.common.constants import (
    CH_DATABASE_CRM,
    CH_DATABASE_HELPDESK,
    CH_DATABASE_WORKFORCE,
    CH_DATABASE_PBX,
    CH_DATABASE_BOT,
    CH_DATABASE_DIALER,
)

_CONFIG_ROOT = Path(__file__).resolve().parents[1]

DOMAIN_DATABASES = {
    "helpdesk": CH_DATABASE_HELPDESK,
    "crm": CH_DATABASE_CRM,
    "pbx": CH_DATABASE_PBX,
    "dialer": CH_DATABASE_DIALER,
    "gs": CH_DATABASE_WORKFORCE,
    "bot": CH_DATABASE_BOT,
}

# Coordinator gate (emitted when hourly FSD ingest batch is complete).
FSD_HOURLY_READY = Asset("dwh://fsd/hourly_ready")

# API entity → ClickHouse cur_*_v table (non-obvious aliases).
_API_CUR_VIEW_TABLE = {
    ("crm", "deal_statuses"): "cur_statuses_v",
}

# GS entity → table name (entity name matches alias in dbt).
_GS_CUR_VIEW_TABLE = {
    "esp_techsup_schedule": "esp_techsup_schedule_v",
    "akc1_techsup_schedule": "akc1_techsup_schedule_v",
    "akc2_techsup_schedule": "akc2_techsup_schedule_v",
    "esp_techsup_activities": "esp_techsup_activities_v",
    "employee_audit_tickets_sec": "employee_audit_tickets_sec_v",
    "esp_techsup_employees": "esp_techsup_employees_v",
    "akc_employees": "akc_employees_v",
    "itm_msb_targets": "itm_msb_targets_v",
    "calltrafic1_techsup_schedule": "calltrafic1_techsup_schedule_v",
    "calltrafic_employees": "calltrafic_employees_v",
}


def ch_table(database: str, table: str) -> Asset:
    return Asset(f"clickhouse://{database}/{table}")


def fsd_cur_view_asset(entity_cfg: dict) -> Asset:
    alias = entity_cfg.get("alias", entity_cfg["entity"])
    return ch_table(CH_DATABASE_HELPDESK, f"cur_{alias}_v")


def bot_cur_view_asset(entity_cfg: dict) -> Asset:
    alias = entity_cfg.get("alias", entity_cfg["entity"])
    return ch_table(CH_DATABASE_BOT, f"cur_{alias}_v")


def api_cur_view_asset(domain: str, entity: str) -> Asset:
    table = _API_CUR_VIEW_TABLE.get((domain, entity), f"cur_{entity}_v")
    return ch_table(DOMAIN_DATABASES[domain], table)


def gs_cur_view_asset(entity: str) -> Asset:
    table = _GS_CUR_VIEW_TABLE.get(entity, f"{entity}_v")
    return ch_table(CH_DATABASE_WORKFORCE, table)


def mart_asset(database: str, table: str) -> Asset:
    return ch_table(database, table)


def asset_all(*assets: Asset) -> Asset:
    """AND-combine assets for Airflow 3 schedule expressions."""
    if not assets:
        raise ValueError("asset_all() requires at least one asset")
    expr = assets[0]
    for asset in assets[1:]:
        expr = expr & asset
    return expr


# --- Consumer schedules (asset expressions) ---

MART_FSD_TICKET_ACTIONS = ch_table(CH_DATABASE_HELPDESK, "mart_ticket_actions")
MART_GS_EMPLOYEES = ch_table(CH_DATABASE_WORKFORCE, "mart_employees")
MART_GS_EMPLOYEE_DAILY = ch_table(CH_DATABASE_WORKFORCE, "mart_employee_daily_totals")
SKOROZVON_CALLS_DETAIL = ch_table(CH_DATABASE_DIALER, "cur_calls_detail")
MANGO_CALLS_DETAIL = ch_table(CH_DATABASE_PBX, "cur_calls_detail")

GS_MART_SCHEDULE_ASSETS = asset_all(
    gs_cur_view_asset("esp_techsup_employees"),
    gs_cur_view_asset("akc_employees"),
    gs_cur_view_asset("esp_techsup_schedule"),
    gs_cur_view_asset("akc1_techsup_schedule"),
    gs_cur_view_asset("akc2_techsup_schedule"),
    gs_cur_view_asset("esp_techsup_activities"),
    gs_cur_view_asset("employee_audit_tickets_sec"),
    gs_cur_view_asset("esp_test_results"),
    MART_FSD_TICKET_ACTIONS,
    SKOROZVON_CALLS_DETAIL,
)

SKOROZVON_MART_SCHEDULE_ASSETS = asset_all(
    ch_table(CH_DATABASE_DIALER, "cur_calls_v"),
    ch_table(CH_DATABASE_DIALER, "cur_hours_v"),
)

# After updating the calls and directories (disconnect from stats_basic — left join, not a blocker).
MANGO_MART_SCHEDULE_ASSETS = asset_all(
    ch_table(CH_DATABASE_PBX, "cur_calls_v"),
    ch_table(CH_DATABASE_PBX, "cur_users_v"),
    ch_table(CH_DATABASE_PBX, "cur_groups_v"),
)

FSD_MART_SCHEDULE_ASSETS = FSD_HOURLY_READY


def fsd_ingest_dag_ids() -> list[str]:
    path = _CONFIG_ROOT / "config" / "fsd_entities.yaml"
    with open(path, encoding="utf-8") as handle:
        entities = yaml.safe_load(handle).get("entities", [])
    return [f"ingest_fsd_{item['entity']}" for item in entities]


# Mart outlet assets (producer after successful dbt run).
FSD_MART_OUTLETS = [
    ch_table(CH_DATABASE_HELPDESK, "mart_tickets_enriched"),
    ch_table(CH_DATABASE_HELPDESK, "mart_tasks_enriched"),
    ch_table(CH_DATABASE_HELPDESK, "mart_ticket_actions"),
    ch_table(CH_DATABASE_HELPDESK, "mart_tickets_backlog"),
    ch_table(CH_DATABASE_HELPDESK, "mart_tasks_backlog"),
    ch_table(CH_DATABASE_HELPDESK, "mart_operational_load_daily"),
    ch_table(CH_DATABASE_HELPDESK, "cur_ticket_lifecycle_events"),
]

GS_MART_OUTLETS = [
    MART_GS_EMPLOYEES,
    MART_GS_EMPLOYEE_DAILY,
]

TECHSUP_WORK_BLOCKS_SCHEDULE_ASSETS = asset_all(
    fsd_cur_view_asset({"entity": "history", "alias": "history"}),
    fsd_cur_view_asset({"entity": "users", "alias": "users"}),
    ch_table(CH_DATABASE_DIALER, "cur_calls_v"),
    ch_table(CH_DATABASE_PBX, "cur_calls_v"),
    gs_cur_view_asset("esp_techsup_schedule"),
    MART_GS_EMPLOYEE_DAILY,
)

TECHSUP_WORK_BLOCKS_MART_OUTLETS = [
    ch_table(CH_DATABASE_HELPDESK, "mart_employee_work_blocks_daily"),
    ch_table(CH_DATABASE_HELPDESK, "mart_employee_work_block_events"),
]

SKOROZVON_MART_OUTLETS = [
    ch_table(CH_DATABASE_DIALER, "operators_daily_weekly_monthly"),
    ch_table(CH_DATABASE_DIALER, "main_metrics"),
]

MANGO_MART_OUTLETS = [
    MANGO_CALLS_DETAIL,
    ch_table(CH_DATABASE_PBX, "main_metrics"),
    ch_table(CH_DATABASE_PBX, "operator_metrics"),
]

BOT_MART_OUTLETS = [
    ch_table(CH_DATABASE_BOT, "mart_bot_daily"),
    ch_table(CH_DATABASE_BOT, "mart_bot_coverage_daily"),
    ch_table(CH_DATABASE_BOT, "mart_bot_theme_daily"),
    ch_table(CH_DATABASE_BOT, "mart_bot_outcome_daily"),
]
