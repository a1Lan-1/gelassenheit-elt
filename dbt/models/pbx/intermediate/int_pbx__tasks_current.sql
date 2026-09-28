{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_tasks',
    engine=ch_engine_replacing(),
    order_by='(task_id)',
    unique_key=['task_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'tasks', 'current'],
  )
}}

with raw_source as (
  select `task_id`, `status`, `event_type`, `priority`, `contact_id`, `deal_id`, `from_user_id`, `to_user_id`, `start_time`, `raw_data`
  from {{ bronze_parquet('pbx', 'tasks', '`task_id` Nullable(String), `status` Nullable(String), `event_type` Nullable(String), `priority` Nullable(String), `contact_id` Nullable(String), `deal_id` Nullable(String), `from_user_id` Nullable(String), `to_user_id` Nullable(String), `start_time` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`task_id`, 'Nullable(String)')) AS `task_id`,
  CAST(`status`, 'Nullable(String)') AS `status`,
  CAST(`event_type`, 'Nullable(String)') AS `event_type`,
  CAST(`priority`, 'Nullable(String)') AS `priority`,
  CAST(`contact_id`, 'Nullable(String)') AS `contact_id`,
  CAST(`deal_id`, 'Nullable(String)') AS `deal_id`,
  CAST(`from_user_id`, 'Nullable(String)') AS `from_user_id`,
  CAST(`to_user_id`, 'Nullable(String)') AS `to_user_id`,
  CAST(`start_time`, 'Nullable(String)') AS `start_time`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`raw_data`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`raw_data`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`raw_data`)), ''), '}'), nullIf(trimBoth(toString(`raw_data`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `raw_data`
from raw_source