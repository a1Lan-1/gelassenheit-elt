{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_appeals',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'appeals', 'current'],
  )
}}

with raw_source as (
  select `id`, `type`, `source`, `appeal_id`, `companion_id`, `account_id`, `person_id`, `entity_id`, `foreign_id`, `foreign_table`, `result_id`, `actual_date`, `record_link`, `created_at`, `updated_at`, `deleted_at`, `parent_id`, `duration`, `vendor_code`, `comment`, `user_id`
  from {{ bronze_parquet('helpdesk', 'appeals', '`id` String, `type` Nullable(String), `source` Nullable(String), `appeal_id` Nullable(String), `companion_id` Nullable(String), `account_id` Nullable(String), `person_id` String, `entity_id` String, `foreign_id` String, `foreign_table` Nullable(String), `result_id` String, `actual_date` String, `record_link` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `parent_id` String, `duration` Nullable(Int32), `vendor_code` Nullable(String), `comment` Nullable(String), `user_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`source`, 'Nullable(String)') AS `source`,
  assumeNotNull(CAST(`appeal_id`, 'Nullable(String)')) AS `appeal_id`,
  assumeNotNull(CAST(`companion_id`, 'Nullable(String)')) AS `companion_id`,
  CAST(`account_id`, 'Nullable(String)') AS `account_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`person_id`)), '')), 'Nullable(UUID)') AS `person_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`entity_id`)), '')), 'Nullable(UUID)') AS `entity_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`foreign_id`)), '')), 'Nullable(UUID)') AS `foreign_id`,
  CAST(`foreign_table`, 'Nullable(String)') AS `foreign_table`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`result_id`)), '')), 'Nullable(UUID)') AS `result_id`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`actual_date`)), ''), 3), 'Nullable(DateTime64(3))') AS `actual_date`,
  CAST(`record_link`, 'Nullable(String)') AS `record_link`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`parent_id`)), '')), 'Nullable(UUID)') AS `parent_id`,
  CAST(`duration`, 'Nullable(Int32)') AS `duration`,
  CAST(`vendor_code`, 'Nullable(String)') AS `vendor_code`,
  CAST(`comment`, 'Nullable(String)') AS `comment`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_id`)), '')), 'Nullable(UUID)') AS `user_id`
from raw_source