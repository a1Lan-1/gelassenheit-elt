{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_sources',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'sources', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `external_id`, `raw_data`
  from {{ bronze_parquet('crm', 'sources', '`id` Nullable(UInt64), `name` Nullable(String), `external_id` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`external_id`, 'Nullable(String)') AS `external_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
