"""Refresh static dbt documentation on the dbt host."""



from __future__ import annotations



from datetime import datetime



from airflow.decorators import dag, task

from airflow.providers.ssh.operators.ssh import SSHOperator



from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT

from dwh.common.dbt_sync_sensor import dbt_sync_sensor





@dag(

    dag_id="dwh_refresh_dbt_docs",

    schedule="@daily",

    start_date=datetime(2026, 6, 4),

    catchup=False,

    tags=["dwh", "dbt", "docs"],

)

def refresh_dbt_docs():

    @task

    def build_docs_command() -> str:

        from dwh.common.ssh import dbt_docs_generate_cmd



        return dbt_docs_generate_cmd()



    wait_sync = dbt_sync_sensor()

    docs_command = build_docs_command()



    generate = SSHOperator(

        task_id="dbt_docs_generate",

        ssh_conn_id=SSH_CONN_DBT,

        pool=DBT_SSH_POOL,

        command="{{ ti.xcom_pull(task_ids='build_docs_command') }}",

        cmd_timeout=1800,

    )



    wait_sync >> docs_command >> generate





refresh_dbt_docs()

