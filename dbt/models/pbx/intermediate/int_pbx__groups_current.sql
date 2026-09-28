{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_groups',
    engine=ch_engine_replacing(),
    order_by='(group_id)',
    unique_key=['group_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'groups', 'current'],
  )
}}

with raw_source as (
  select `group_id`, `name`, `extension`, `operators`, `raw_data`
  from {{ bronze_parquet('pbx', 'groups', '`group_id` Nullable(String), `name` Nullable(String), `extension` Nullable(String), `operators` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`group_id`, 'Nullable(String)')) AS `group_id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`extension`, 'Nullable(String)') AS `extension`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`operators`)), '')") }}, 'Nullable(JSON)') AS `operators`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`raw_data`)), '')") }}, 'Nullable(JSON)') AS `raw_data`
from raw_source
