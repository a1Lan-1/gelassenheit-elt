{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_catalogs',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'catalogs', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `type`, `can_add_elements`, `can_link_multiple`, `raw_data`
  from {{ bronze_parquet('crm', 'catalogs', '`id` Nullable(UInt64), `name` Nullable(String), `type` Nullable(String), `can_add_elements` Nullable(String), `can_link_multiple` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`can_add_elements`, 'Nullable(String)') AS `can_add_elements`,
  CAST(`can_link_multiple`, 'Nullable(String)') AS `can_link_multiple`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
