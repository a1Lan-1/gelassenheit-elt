"""Daily ops: rebuild duplicate-key report and alert when rows exist."""



from __future__ import annotations



from datetime import datetime



from airflow.decorators import dag, task

from airflow.providers.ssh.operators.ssh import SSHOperator



from dwh.common.constants import DBT_SSH_POOL, SSH_CONN_DBT, SSH_DBT_CMD_TIMEOUT

from dwh.common.dbt_sync_sensor import dbt_sync_sensor





@dag(

    dag_id="dwh_ops_cur_duplicate_keys",

    schedule="@daily",

    start_date=datetime(2026, 6, 3),

    catchup=False,

    tags=["dwh", "ops", "duplicate_keys"],

    max_active_runs=1,

)

def ops_cur_duplicate_keys():

    @task

    def build_duplicate_run() -> str:

        from dwh.common.ssh import dbt_run_cmd



        return dbt_run_cmd("ops_cur_duplicate_keys")



    @task

    def alert_if_duplicates() -> dict:

        import json



        from dwh.common.airflow_health import send_telegram

        from dwh.common.ssh import exec_ssh



        from dwh.common.ch_ops import duplicate_keys_alert_script



        code, out, err = exec_ssh(SSH_CONN_DBT, duplicate_keys_alert_script(), timeout=120)

        if code != 0:

            raise RuntimeError(f"duplicate keys check failed: {err or out}")

        count = 0

        for line in out.splitlines():

            if line.startswith("{"):

                count = int(json.loads(line).get("duplicate_rows", 0))

        if count > 0:

            send_telegram(

                f"DWH ops: {count} duplicate primary key row(s) in gel_helpdesk.ops_cur_duplicate_keys"

            )

        return {"duplicate_rows": count}



    wait_sync = dbt_sync_sensor()



    run_dup = SSHOperator(

        task_id="dbt_ops_cur_duplicate_keys",

        ssh_conn_id=SSH_CONN_DBT,

        pool=DBT_SSH_POOL,

        command="{{ ti.xcom_pull(task_ids='build_duplicate_run') }}",

        cmd_timeout=SSH_DBT_CMD_TIMEOUT,

    )

    run_cmd = build_duplicate_run()

    alert = alert_if_duplicates()

    wait_sync >> run_cmd >> run_dup >> alert





ops_cur_duplicate_keys()

