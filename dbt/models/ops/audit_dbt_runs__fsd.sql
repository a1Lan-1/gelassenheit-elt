{#
  DDL for gel_helpdesk.audit_dbt_runs.
  Live path: scripts/log_dbt_run_stats.py after each Airflow dbt run.
  Run once: dbt run --select tag:audit_dbt_runs --full-refresh
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    schema='gel_helpdesk',
    alias='audit_dbt_runs',
    engine=ch_engine_merge_tree(),
    order_by='(run_generated_at, model_name, invocation_id)',
    settings={'allow_nullable_key': 1},
    tags=['ops', 'audit_dbt_runs', 'helpdesk'],
  )
}}

{{ audit_dbt_runs_select('helpdesk') }}
