{#
  Primary path for audit_bronze is live INSERT from Airflow ingest
  (dwh.common.ch_client) after each bronze upload.
  This model is for manual/gap backfill from S3 manifests only -
  do not schedule hourly while live inserts are active.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    schema='gel_workforce',
    alias='audit_bronze',
    unique_key=['source', 'entity', 'bronze_run_id'],
    engine=ch_engine_merge_tree(),
    order_by='(source, entity, bronze_run_id)',
    settings={'allow_nullable_key': 1},
    tags=['ops', 'audit_bronze', 'audit_bronze_backfill', 'gs'],
  )
}}

{% set entities = ['esp_techsup_schedule', 'akc1_techsup_schedule', 'esp_techsup_activities', 'employee_audit_tickets_sec', 'esp_techsup_employees', 'akc2_techsup_schedule', 'akc_employees', 'itm_msb_targets', 'esp_test_results', 'calltrafic1_techsup_schedule', 'calltrafic_employees'] %}
{% set source_timezone = 'MSK' %}
select *
from (
{{ bronze_manifests_union('gs', entities, source_timezone) }}
)
settings input_format_json_infer_incomplete_types_as_strings = 1
