{# ReplacingMergeTree: grandpa key = ORDER BY (id); updated_at version only. #}
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_tickets_enriched',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    partition_by='toYYYYMM(created_at)',
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'tickets_enriched', 'tickets_enriched_core', 'current'],
  )
}}

{{ fsd_tickets_enriched_base() }}
