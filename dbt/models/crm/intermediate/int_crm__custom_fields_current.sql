{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_custom_fields',
    engine=ch_engine_replacing(),
    order_by='(entity_type, id)',
    unique_key=['entity_type', 'id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'custom_fields', 'current'],
  )
}}

with raw_source as (
  select `id`, `entity_type`, `name`, `code`, `type`, `sort`, `is_api_only`, `raw_data`
  from {{ bronze_parquet('crm', 'custom_fields', '`id` Nullable(UInt64), `entity_type` Nullable(String), `name` Nullable(String), `code` Nullable(String), `type` Nullable(String), `sort` Nullable(Int32), `is_api_only` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  assumeNotNull(CAST(`entity_type`, 'Nullable(String)')) AS `entity_type`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`code`, 'Nullable(String)') AS `code`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`sort`, 'Nullable(Int32)') AS `sort`,
  CAST(`is_api_only`, 'Nullable(String)') AS `is_api_only`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
