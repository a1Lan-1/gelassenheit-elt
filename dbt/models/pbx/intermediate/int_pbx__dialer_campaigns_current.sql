{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_dialer_campaigns',
    engine=ch_engine_replacing(),
    order_by='(campaign_id)',
    unique_key=['campaign_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'dialer_campaigns', 'current'],
  )
}}

with raw_source as (
  select `campaign_id`, `name`, `status`, `priority`, `line_id`, `start_ts`, `end_ts`, `created_ts`, `created_by_user_id`, `tasks_count`, `finished_tasks_count`, `members`, `operators`, `raw_data`
  from {{ bronze_parquet('pbx', 'dialer_campaigns', '`campaign_id` Nullable(String), `name` Nullable(String), `status` Nullable(String), `priority` Nullable(String), `line_id` Nullable(String), `start_ts` Nullable(String), `end_ts` Nullable(String), `created_ts` Nullable(String), `created_by_user_id` Nullable(String), `tasks_count` Nullable(Int64), `finished_tasks_count` Nullable(Int64), `members` Nullable(String), `operators` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`campaign_id`, 'Nullable(String)')) AS `campaign_id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`status`, 'Nullable(String)') AS `status`,
  CAST(`priority`, 'Nullable(String)') AS `priority`,
  CAST(`line_id`, 'Nullable(String)') AS `line_id`,
  CAST(`start_ts`, 'Nullable(String)') AS `start_ts`,
  CAST(`end_ts`, 'Nullable(String)') AS `end_ts`,
  CAST(`created_ts`, 'Nullable(String)') AS `created_ts`,
  CAST(`created_by_user_id`, 'Nullable(String)') AS `created_by_user_id`,
  CAST(`tasks_count`, 'Nullable(Int64)') AS `tasks_count`,
  CAST(`finished_tasks_count`, 'Nullable(Int64)') AS `finished_tasks_count`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`members`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`members`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`members`)), ''), '}'), nullIf(trimBoth(toString(`members`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `members`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`operators`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`operators`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`operators`)), ''), '}'), nullIf(trimBoth(toString(`operators`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `operators`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`raw_data`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`raw_data`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`raw_data`)), ''), '}'), nullIf(trimBoth(toString(`raw_data`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `raw_data`
from raw_source