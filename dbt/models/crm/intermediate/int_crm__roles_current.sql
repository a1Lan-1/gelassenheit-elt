{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_roles',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'roles', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `raw_data`
  from {{ bronze_parquet('crm', 'roles', '`id` Nullable(UInt64), `name` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
