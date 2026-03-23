{{
  config(
    materialized='table',
    alias='stg_statuses',
    engine=ch_engine_merge_tree(),
    order_by='(id)',
    settings={'allow_nullable_key': 1},
    tags=['crm', 'deal_statuses', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('crm', 'deal_statuses', '`id` Nullable(UInt64), `name` Nullable(String), `sort` Nullable(Int32), `pipeline_id` Nullable(UInt64)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`sort`, 'Nullable(Int32)') AS `sort`,
  CAST(`pipeline_id`, 'Nullable(UInt64)') AS `pipeline_id`
from raw_source
