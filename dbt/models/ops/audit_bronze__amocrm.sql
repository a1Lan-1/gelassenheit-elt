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
    schema='gel_crm',
    alias='audit_bronze',
    unique_key=['source', 'entity', 'bronze_run_id'],
    engine=ch_engine_merge_tree(),
    order_by='(source, entity, bronze_run_id)',
    settings={'allow_nullable_key': 1},
    tags=['ops', 'audit_bronze', 'audit_bronze_backfill', 'crm'],
  )
}}

{% set entities = ['deals', 'companies', 'users', 'deal_statuses', 'events', 'pipelines', 'loss_reasons', 'roles', 'sources', 'tags', 'custom_fields', 'catalogs', 'contacts', 'tasks', 'notes', 'unsorted', 'customers', 'catalog_elements'] %}
{% set source_timezone = 'UTC' %}
select *
from (
{{ bronze_manifests_union('crm', entities, source_timezone) }}
)
settings input_format_json_infer_incomplete_types_as_strings = 1
