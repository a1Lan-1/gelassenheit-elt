"""Airflow schedule helpers (Europe/Moscow for cron expressions)."""

from __future__ import annotations

FSD_INGEST_CRON_MSK = "0 * * * *"
GS_INGEST_CRON_MSK = "0 * * * *"
GS_MARTS_CRON_MSK = "56 * * * *"
FSD_RECONCILE_CRON_MSK = "48 3 * * *"
FSD_MARTS_CRON_MSK = "54 * * * *"
ESP_MARTS_CRON_MSK = "57 * * * *"
ESP_LICENSE_FUNNEL_CRON_MSK = "0 4 * * *"
ESP_KKT_INFO_CRON_MSK = "0 5 * * *"
SKOROZVON_YEARLY_FULL_REFRESH_CRON_MSK = "0 4 1 1 *"
DBT_YEARLY_FULL_REFRESH_CRON_MSK = "0 2 1 1 *"
FSD_HOURLY_READY_CRON_MSK = "45 * * * *"
TECHSUP_WORK_BLOCKS_CRON_MSK = "58 * * * *"
FSD_TICKET_GEO_CRON_MSK = "0 * * * *"
CASHDESK_GEO_CRON_MSK = "30 6 * * *"
SKOROZVON_MARTS_CRON_MSK = "20 * * * *"
BOT_MARTS_CRON_MSK = "25 * * * *"
# Recommendation P4 / hard-cut: slots=2 only after wave F criteria (lifecycle read <2GB).
# Now we leave 1; the constant is the target value after the measurement.
DBT_HOST_SLOTS_RECOMMENDED = 2
DBT_HOST_SLOTS_ENABLED = 1
FSD_INGEST_TIMEZONE = "Europe/Moscow"


def cron_timetable(cron: str) -> object:
    """Wrap cron expression in CronTriggerTimetable (required for AssetOrTimeSchedule)."""
    from airflow.timetables.trigger import CronTriggerTimetable

    return CronTriggerTimetable(cron, timezone=FSD_INGEST_TIMEZONE)


def resolve_schedule(schedule: str | None) -> str | object:
    """Apply MSK timezone to cron from YAML; pass through Airflow presets (@daily, etc.)."""
    cron = (schedule or FSD_INGEST_CRON_MSK).strip()
    if cron.startswith("@"):
        return cron
    return cron_timetable(cron)


def fsd_entity_schedule(override: str | None = None) -> str | object:
    """Hourly ingest at :00 MSK; per-entity override via YAML schedule."""
    return resolve_schedule(override)


def hybrid_schedule(cron: str | object, asset_trigger: object) -> object:
    """Cron SLA fallback plus asset-driven trigger (Airflow 3)."""
    from airflow.timetables.assets import AssetOrTimeSchedule

    timetable = cron_timetable(cron) if isinstance(cron, str) else cron
    return AssetOrTimeSchedule(timetable=timetable, assets=asset_trigger)


def gs_entity_schedule(override: str | None = None) -> str | object:
    """Hourly GS ingest at :00 MSK; per-entity override via YAML schedule."""
    return resolve_schedule(override or GS_INGEST_CRON_MSK)
