{#
  DDL for gel_crm.audit_dbt_runs.
  Live path: scripts/log_dbt_run_stats.py after each Airflow dbt run.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    schema='gel_crm',
    alias='audit_dbt_runs',
    engine=ch_engine_merge_tree(),
    order_by='(run_generated_at, model_name, invocation_id)',
    settings={'allow_nullable_key': 1},
    tags=['ops', 'audit_dbt_runs', 'crm'],
  )
}}

{{ audit_dbt_runs_select('crm') }}
