"""Synchronize gelassenheit-dbt repository and package deps on the dbt host."""



from __future__ import annotations



from datetime import datetime



from airflow.decorators import dag, task

from airflow.providers.ssh.operators.ssh import SSHOperator



from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT





@dag(

    dag_id="dwh_sync_dbt_repo",

    schedule="*/5 * * * *",

    start_date=datetime(2026, 6, 4),

    catchup=False,

    tags=["dwh", "dbt", "deploy"],

    max_active_runs=1,

)

def sync_dbt_repo():

    @task

    def build_sync_command() -> str:

        from dwh.common.ssh import dbt_sync_and_deps_cmd



        return dbt_sync_and_deps_cmd()



    sync_command = build_sync_command()



    sync = SSHOperator(

        task_id="git_sync_dbt_etl",

        ssh_conn_id=SSH_CONN_DBT,

        pool=DBT_SSH_POOL,

        command="{{ ti.xcom_pull(task_ids='build_sync_command') }}",

        cmd_timeout=900,

    )



    sync_command >> sync





sync_dbt_repo()

