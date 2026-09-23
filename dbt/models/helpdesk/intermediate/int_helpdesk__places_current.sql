{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_places',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'places', 'current'],
  )
}}

with raw_source as (
  select `id`, `external_id`, `entity_id`, `region`, `city`, `address`, `fias_code`, `latitude`, `longtitude`, `description`, `responsible_person_id`, `data`, `created_at`, `updated_at`, `deleted_at`, `type`, `postal`, `area`, `street`, `house`, `room`, `post_delivery_valid`, `time_zone`
  from {{ bronze_parquet('helpdesk', 'places', '`id` String, `external_id` Nullable(String), `entity_id` String, `region` Nullable(String), `city` Nullable(String), `address` Nullable(String), `fias_code` Nullable(String), `latitude` Nullable(String), `longtitude` Nullable(String), `description` Nullable(String), `responsible_person_id` String, `data` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `type` Nullable(String), `postal` Nullable(Int32), `area` Nullable(String), `street` Nullable(String), `house` Nullable(String), `room` Nullable(String), `post_delivery_valid` Nullable(String), `time_zone` Nullable(String)') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(`external_id`, 'Nullable(String)') AS `external_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`entity_id`)), '')), 'Nullable(UUID)') AS `entity_id`,
  CAST(`region`, 'Nullable(String)') AS `region`,
  CAST(`city`, 'Nullable(String)') AS `city`,
  CAST(`address`, 'Nullable(String)') AS `address`,
  CAST(`fias_code`, 'Nullable(String)') AS `fias_code`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`latitude`)), ''), ',', '.')), 'Nullable(Float64)') AS `latitude`,
  CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`longtitude`)), ''), ',', '.')), 'Nullable(Float64)') AS `longtitude`,
  CAST(`description`, 'Nullable(String)') AS `description`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`responsible_person_id`)), '')), 'Nullable(UUID)') AS `responsible_person_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  assumeNotNull(CAST(`type`, 'Nullable(String)')) AS `type`,
  CAST(`postal`, 'Nullable(Int32)') AS `postal`,
  CAST(`area`, 'Nullable(String)') AS `area`,
  CAST(`street`, 'Nullable(String)') AS `street`,
  CAST(`house`, 'Nullable(String)') AS `house`,
  CAST(`room`, 'Nullable(String)') AS `room`,
  assumeNotNull(CAST(multiIf(lower(trimBoth(toString(`post_delivery_valid`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`post_delivery_valid`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`post_delivery_valid`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`post_delivery_valid`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)')) AS `post_delivery_valid`,
  CAST(`time_zone`, 'Nullable(String)') AS `time_zone`
from raw_source