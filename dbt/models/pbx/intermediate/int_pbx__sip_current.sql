{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_sip',
    engine=ch_engine_replacing(),
    order_by='(login)',
    unique_key=['login'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'sip', 'current'],
  )
}}

with raw_source as (
  select `login`, `user_id`, `extension`, `name`, `raw_data`
  from {{ bronze_parquet('pbx', 'sip', '`login` Nullable(String), `user_id` Nullable(String), `extension` Nullable(String), `name` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`login`, 'Nullable(String)')) AS `login`,
  CAST(`user_id`, 'Nullable(String)') AS `user_id`,
  CAST(`extension`, 'Nullable(String)') AS `extension`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
