{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_users',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'users', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `email`
  from {{ bronze_parquet('crm', 'users', '`id` Nullable(UInt64), `name` Nullable(String), `email` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`email`, 'Nullable(String)') AS `email`
from raw_source