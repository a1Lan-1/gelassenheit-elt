{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_pipelines',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'pipelines', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `sort`, `is_main`, `is_archive`, `account_id`, `raw_data`
  from {{ bronze_parquet('crm', 'pipelines', '`id` Nullable(UInt64), `name` Nullable(String), `sort` Nullable(Int32), `is_main` Nullable(String), `is_archive` Nullable(String), `account_id` Nullable(UInt64), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`sort`, 'Nullable(Int32)') AS `sort`,
  CAST(`is_main`, 'Nullable(String)') AS `is_main`,
  CAST(`is_archive`, 'Nullable(String)') AS `is_archive`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
