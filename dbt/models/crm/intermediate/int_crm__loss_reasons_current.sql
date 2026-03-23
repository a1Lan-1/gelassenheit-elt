{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_loss_reasons',
    engine=ch_engine_replacing(),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'loss_reasons', 'current'],
  )
}}

with raw_source as (
  select `id`, `name`, `sort`, `account_id`, `raw_data`
  from {{ bronze_parquet('crm', 'loss_reasons', '`id` Nullable(UInt64), `name` Nullable(String), `sort` Nullable(Int32), `account_id` Nullable(UInt64), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`sort`, 'Nullable(Int32)') AS `sort`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
