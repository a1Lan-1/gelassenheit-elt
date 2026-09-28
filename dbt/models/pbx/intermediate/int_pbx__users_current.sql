{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_users',
    engine=ch_engine_replacing(),
    order_by='(extension)',
    unique_key=['extension'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'users', 'current'],
  )
}}

with raw_source as (
  select `extension`, `name`, `email`, `department`, `position`, `user_id`, `access_role_id`, `mobile`, `groups`, `sips`, `raw_data`
  from {{ bronze_parquet('pbx', 'users', '`extension` Nullable(String), `name` Nullable(String), `email` Nullable(String), `department` Nullable(String), `position` Nullable(String), `user_id` Nullable(String), `access_role_id` Nullable(String), `mobile` Nullable(String), `groups` Nullable(String), `sips` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`extension`, 'Nullable(String)')) AS `extension`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`email`, 'Nullable(String)') AS `email`,
  CAST(`department`, 'Nullable(String)') AS `department`,
  CAST(`position`, 'Nullable(String)') AS `position`,
  CAST(`user_id`, 'Nullable(String)') AS `user_id`,
  CAST(`access_role_id`, 'Nullable(String)') AS `access_role_id`,
  CAST(`mobile`, 'Nullable(String)') AS `mobile`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`groups`)), '')") }}, 'Nullable(JSON)') AS `groups`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`sips`)), '')") }}, 'Nullable(JSON)') AS `sips`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
