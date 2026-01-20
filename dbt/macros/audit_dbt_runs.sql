{# Empty SELECT that defines sync_*.audit_dbt_runs schema (live path: scripts/log_dbt_run_stats.py). #}
{% macro audit_dbt_runs_select(source_name) -%}
select
  cast('{{ source_name }}' as String) as source,
  cast('' as String) as model_name,
  cast('' as String) as unique_id,
  cast('' as String) as status,
  cast(0 as Float64) as execution_time_s,
  cast(null as Nullable(Int64)) as rows_affected,
  cast(null as Nullable(String)) as message,
  cast('' as String) as invocation_id,
  cast(null as Nullable(String)) as airflow_dag_id,
  cast(null as Nullable(String)) as airflow_run_id,
  cast(null as Nullable(DateTime64(3))) as run_generated_at,
  now64(3) as inserted_at
where 1 = 0
{%- endmacro %}
