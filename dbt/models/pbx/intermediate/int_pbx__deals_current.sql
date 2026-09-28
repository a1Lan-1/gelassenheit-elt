{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_deals',
    engine=ch_engine_replacing(),
    order_by='(deal_id)',
    unique_key=['deal_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['pbx', 'deals', 'current'],
  )
}}

with raw_source as (
  select `deal_id`, `name`, `description`, `amount`, `abonent_id`, `contact_id`, `funnel_id`, `step_id`, `status`, `status_change_reason`, `reason_comment`, `custom_fields`, `raw_data`
  from {{ bronze_parquet('pbx', 'deals', '`deal_id` Nullable(String), `name` Nullable(String), `description` Nullable(String), `amount` Nullable(String), `abonent_id` Nullable(String), `contact_id` Nullable(String), `funnel_id` Nullable(String), `step_id` Nullable(String), `status` Nullable(String), `status_change_reason` Nullable(String), `reason_comment` Nullable(String), `custom_fields` Nullable(String), `raw_data` Nullable(String)') }}
)
select
  assumeNotNull(CAST(`deal_id`, 'Nullable(String)')) AS `deal_id`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`description`, 'Nullable(String)') AS `description`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`amount`)), ''), ',', '.')), 'Nullable(Float64)') AS `amount`,
  CAST(`abonent_id`, 'Nullable(String)') AS `abonent_id`,
  CAST(`contact_id`, 'Nullable(String)') AS `contact_id`,
  CAST(`funnel_id`, 'Nullable(String)') AS `funnel_id`,
  CAST(`step_id`, 'Nullable(String)') AS `step_id`,
  CAST(`status`, 'Nullable(String)') AS `status`,
  CAST(`status_change_reason`, 'Nullable(String)') AS `status_change_reason`,
  CAST(`reason_comment`, 'Nullable(String)') AS `reason_comment`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`custom_fields`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`custom_fields`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`custom_fields`)), ''), '}'), nullIf(trimBoth(toString(`custom_fields`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `custom_fields`,
  CAST(if(isValidJSON(nullIf(trimBoth(toString(`raw_data`)), '')), accurateCastOrNull(if(startsWith(ltrim(nullIf(trimBoth(toString(`raw_data`)), '')), '['), concat('{"items":', nullIf(trimBoth(toString(`raw_data`)), ''), '}'), nullIf(trimBoth(toString(`raw_data`)), '')), 'JSON'), NULL), 'Nullable(JSON)') AS `raw_data`
from raw_source