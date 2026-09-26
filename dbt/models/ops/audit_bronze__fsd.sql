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
    schema='gel_helpdesk',
    alias='audit_bronze',
    unique_key=['source', 'entity', 'bronze_run_id'],
    engine=ch_engine_merge_tree(),
    order_by='(source, entity, bronze_run_id)',
    settings={'allow_nullable_key': 1},
    tags=['ops', 'audit_bronze', 'audit_bronze_backfill', 'helpdesk'],
  )
}}

{% set entities = ['appeals', 'comments', 'config_items', 'entities', 'history', 'persons', 'places', 'services', 'sku', 'statuses', 'task_config_item', 'task_status', 'tasks', 'ticket_cases', 'ticket_categories', 'ticket_channels', 'ticket_config_item', 'ticket_status', 'tickets', 'types', 'user_groups', 'user_groups_mapping', 'user_types', 'users'] %}
{% set source_timezone = 'MSK' %}
select *
from (
{{ bronze_manifests_union('helpdesk', entities, source_timezone) }}
)
settings input_format_json_infer_incomplete_types_as_strings = 1
