{{
  config(
    materialized='table',
    alias='stg_users',
    engine=ch_engine_merge_tree(),
    order_by='(id)',
    settings={'allow_nullable_key': 1},
    tags=['crm', 'users', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('crm', 'users', '`id` Nullable(UInt64), `name` Nullable(String), `email` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`email`, 'Nullable(String)') AS `email`
from raw_source
