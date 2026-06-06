"""Factory: API domain ingest DAGs → bronze → dbt current."""



from __future__ import annotations



from datetime import datetime, timedelta

from pathlib import Path



import yaml

from airflow.decorators import dag, task

from airflow.providers.ssh.operators.ssh import SSHOperator



from dwh.common.constants import (
    API_EXPORT_MAX_RETRY_DELAY_MIN,
    API_EXPORT_RETRIES,
    API_EXPORT_RETRY_DELAY_MIN,
    DBT_SSH_POOL,
    SSH_CONN_DBT,
    SSH_DBT_CMD_TIMEOUT,
)

from dwh.common.dbt_sync_sensor import dbt_sync_sensor
from dwh.common.assets import api_cur_view_asset
from dwh.common.schedules import resolve_schedule



CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "api_entities.yaml"



DOMAIN_EXTRACTOR_MODULES = {

    "crm": "dwh.ingest.api.extractors.crm",

    "pbx": "dwh.ingest.api.extractors.pbx",

    "dialer": "dwh.ingest.api.extractors.dialer",

}





def _load_domains() -> dict:

    with open(CONFIG_PATH, encoding="utf-8") as handle:

        return yaml.safe_load(handle)["domains"]





def _dbt_current_model(domain: str, entity: str) -> str:

    return f"int_{domain}__{entity}_current"





def _build_dag(domain: str, entity_cfg: dict, schedule: str | object):

    entity = entity_cfg["entity"]

    model = _dbt_current_model(domain, entity)

    view_model = f"{model}_v"

    dag_id = f"ingest_{domain}_{entity}"

    cur_asset = api_cur_view_asset(domain, entity)



    @dag(

        dag_id=dag_id,

        schedule=schedule,

        start_date=datetime(2026, 6, 3),

        catchup=False,

        tags=["dwh", domain, "ingest", entity],

        max_active_runs=1,

    )

    def _ingest():

        @task(
            retries=API_EXPORT_RETRIES,
            retry_delay=timedelta(minutes=API_EXPORT_RETRY_DELAY_MIN),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=API_EXPORT_MAX_RETRY_DELAY_MIN),
        )
        def extract_and_upload(**context) -> dict:

            import importlib



            from dwh.common.api_ingest import run_api_ingest_task



            module_name = DOMAIN_EXTRACTOR_MODULES.get(domain)

            if module_name is None:

                raise NotImplementedError(f"No extractor module for {domain}")

            extractors = importlib.import_module(module_name).EXTRACTORS

            extract_fn = extractors.get(entity)

            if extract_fn is None:

                raise NotImplementedError(f"No extractor for {domain}.{entity}")

            return run_api_ingest_task(

                domain=domain,

                entity=entity,

                extract_fn=extract_fn,

                dag_id=context["dag"].dag_id,

                airflow_run_id=context["run_id"],

                ds=context["ds"],

            )



        @task.short_circuit

        def has_bronze_rows(meta: dict) -> bool:

            return int(meta.get("row_count", 0)) > 0



        @task

        def build_dbt_run_command(meta: dict) -> str:

            from dwh.common.ssh import dbt_run_cmd



            return dbt_run_cmd(

                model,

                bronze_dt=meta["dt"],

                bronze_run_id=meta["run_id"],

                with_current_view=True,

            )



        @task

        def build_dbt_test_command() -> str:

            from dwh.common.ssh import dbt_test_cmd



            return dbt_test_cmd(model, with_current_view=True)



        @task(trigger_rule="none_failed_min_one_success")

        def watermark(meta: dict) -> dict:

            from dwh.common.api_ingest import update_api_watermark



            return update_api_watermark(meta)



        wait_sync = dbt_sync_sensor()



        run_dbt = SSHOperator(

            task_id=f"dbt_run_{model}",

            ssh_conn_id=SSH_CONN_DBT,

            pool=DBT_SSH_POOL,

            command="{{ ti.xcom_pull(task_ids='build_dbt_run_command') }}",

            cmd_timeout=SSH_DBT_CMD_TIMEOUT,

        )

        test_dbt = SSHOperator(

            task_id=f"dbt_test_{model}",

            ssh_conn_id=SSH_CONN_DBT,

            pool=DBT_SSH_POOL,

            command="{{ ti.xcom_pull(task_ids='build_dbt_test_command') }}",

            cmd_timeout=SSH_DBT_CMD_TIMEOUT,

            outlets=[cur_asset],

        )



        meta = extract_and_upload()

        gate = has_bronze_rows(meta)

        dbt_run_command = build_dbt_run_command(meta)

        dbt_test_command = build_dbt_test_command()

        done = watermark(meta)



        meta >> wait_sync >> gate

        gate >> dbt_run_command >> run_dbt >> dbt_test_command >> test_dbt >> done
        wait_sync >> done



    return _ingest()





for _domain, _cfg in _load_domains().items():

    _schedule = resolve_schedule(_cfg.get("schedule", "@hourly"))

    for _entity_cfg in _cfg.get("entities", []):

        _dag = _build_dag(_domain, _entity_cfg, _schedule)

        globals()[_dag.dag_id] = _dag

