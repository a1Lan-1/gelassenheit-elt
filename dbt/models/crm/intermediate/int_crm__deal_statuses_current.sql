{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_statuses',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'deal_statuses', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `sort`, `pipeline_id`
  from {{ bronze_parquet('crm', 'deal_statuses', '`id` Nullable(UInt64), `name` Nullable(String), `sort` Nullable(Int32), `pipeline_id` Nullable(UInt64)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`sort`, 'Nullable(Int32)') AS `sort`,
  CAST(`pipeline_id`, 'Nullable(UInt64)') AS `pipeline_id`
from raw_source