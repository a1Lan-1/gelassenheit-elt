{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_dialer_tasks',
    engine=ch_engine_replacing(),
    order_by='(task_id)',
    unique_key=['task_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'dialer_tasks', 'current'],
  )
}}

with raw_source as (
  select `task_id`, `campaign_id`, `number`, `name`, `task_status`, `operator_id`, `attempts_count`, `task_created`, `task_updated`, `raw_data`
  from {{ bronze_parquet('pbx', 'dialer_tasks', '`task_id` Nullable(String), `campaign_id` Nullable(String), `number` Nullable(String), `name` Nullable(String), `task_status` Nullable(String), `operator_id` Nullable(String), `attempts_count` Nullable(Int64), `task_created` Nullable(String), `task_updated` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`task_id`, 'Nullable(String)')) AS `task_id`,
  CAST(`campaign_id`, 'Nullable(String)') AS `campaign_id`,
  CAST(`number`, 'Nullable(String)') AS `number`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`task_status`, 'Nullable(String)') AS `task_status`,
  CAST(`operator_id`, 'Nullable(String)') AS `operator_id`,
  CAST(`attempts_count`, 'Nullable(Int64)') AS `attempts_count`,
  CAST(`task_created`, 'Nullable(String)') AS `task_created`,
  CAST(`task_updated`, 'Nullable(String)') AS `task_updated`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`raw_data`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`raw_data`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`raw_data`)), ''), '}'), nullIf(trimBoth(toString(`raw_data`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `raw_data`
from raw_source