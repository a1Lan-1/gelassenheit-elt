{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_tags',
    engine=ch_engine_replacing(),
    order_by='(entity_type, id)',
    unique_key=['entity_type', 'id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'tags', 'current'],
  )
}}

with raw_source as (
  select `id`, `entity_type`, `name`, `color`, `raw_data`
  from {{ bronze_parquet('crm', 'tags', '`id` Nullable(UInt64), `entity_type` Nullable(String), `name` Nullable(String), `color` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  assumeNotNull(CAST(`entity_type`, 'Nullable(String)')) AS `entity_type`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`color`, 'Nullable(String)') AS `color`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
