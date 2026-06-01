"""Factory: register ingest DAGs for all FSD entities from YAML config."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from airflow.decorators import dag, task

from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.assets import fsd_cur_view_asset
from dwh.common.schedules import fsd_entity_schedule

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "fsd_entities.yaml"


def _load_entities() -> list[dict]:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data["entities"]


def _dbt_current_model(entity: str) -> str:
    return f"int_helpdesk__{entity}_current"


def _build_dag(entity_cfg: dict):
    entity = entity_cfg["entity"]
    dag_id = f"ingest_fsd_{entity}"
    model = _dbt_current_model(entity)
    schedule = fsd_entity_schedule(entity_cfg.get("schedule"))
    max_runs = 1 if entity == "history" else 1
    cur_asset = fsd_cur_view_asset(entity_cfg)

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=datetime(2026, 6, 3),
        catchup=False,
        tags=["dwh", "helpdesk", "ingest", entity],
        max_active_runs=max_runs,
    )
    def _ingest():
        @task
        def extract_and_upload_batches(**context) -> list[dict]:
            from dwh.common.fsd_ingest import extract_and_upload_batches

            return extract_and_upload_batches(
                entity_cfg,
                dag_id=context["dag"].dag_id,
                airflow_run_id=context["run_id"],
                ds=context["ds"],
            )

        @task.short_circuit
        def has_bronze_rows(batches: list[dict]) -> bool:
            return any(int(batch.get("row_count", 0)) > 0 for batch in batches)

        @task
        def run_dbt_batches(**context) -> list[dict]:
            from dwh.common.fsd_ingest import run_dbt_for_batches

            batches = context["ti"].xcom_pull(task_ids="extract_and_upload_batches") or []
            run_dbt_for_batches(batches, model=model)
            return batches

        @task(outlets=[cur_asset])
        def watermark(batches: list[dict]) -> dict:
            from dwh.common.fsd_ingest import update_watermark

            if not batches:
                return {}
            return update_watermark(batches[-1])

        wait_sync = dbt_sync_sensor()

        batches = extract_and_upload_batches()
        gate = has_bronze_rows(batches)
        dbt_done = run_dbt_batches()
        done = watermark(dbt_done)

        batches >> wait_sync >> gate >> dbt_done >> done

    return _ingest()


for _entity_cfg in _load_entities():
    _dag = _build_dag(_entity_cfg)
    globals()[_dag.dag_id] = _dag
