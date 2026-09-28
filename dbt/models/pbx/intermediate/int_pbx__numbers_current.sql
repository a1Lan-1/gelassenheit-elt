{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_numbers',
    engine=ch_engine_replacing(),
    order_by='(line_id)',
    unique_key=['line_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'numbers', 'current'],
  )
}}

with raw_source as (
  select `line_id`, `number`, `name`, `comment`, `region`, `schema_id`, `schema_name`, `raw_data`
  from {{ bronze_parquet('pbx', 'numbers', '`line_id` Nullable(String), `number` Nullable(String), `name` Nullable(String), `comment` Nullable(String), `region` Nullable(String), `schema_id` Nullable(String), `schema_name` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`line_id`, 'Nullable(String)')) AS `line_id`,
  CAST(`number`, 'Nullable(String)') AS `number`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`comment`, 'Nullable(String)') AS `comment`,
  CAST(`region`, 'Nullable(String)') AS `region`,
  CAST(`schema_id`, 'Nullable(String)') AS `schema_id`,
  CAST(`schema_name`, 'Nullable(String)') AS `schema_name`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
