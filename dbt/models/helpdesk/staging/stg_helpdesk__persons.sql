{{
  config(
    materialized='view',
    alias='stg_persons',
    tags=['helpdesk', 'persons', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `foreign_id`, `table_name`, `external_id`, `first_name`, `middle_name`, `last_name`, `position`, `created_at`, `updated_at`, `deleted_at`, `role`, `working_schedule`, `authority_id`
  from {{ s3_parquet_run('helpdesk', 'persons', '`id` String, `foreign_id` String, `table_name` Nullable(String), `external_id` Nullable(String), `first_name` Nullable(String), `middle_name` Nullable(String), `last_name` Nullable(String), `position` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `role` Nullable(String), `working_schedule` Nullable(String), `authority_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`foreign_id`)), '')), 'Nullable(UUID)') AS `foreign_id`,
  CAST(`table_name`, 'Nullable(String)') AS `table_name`,
  CAST(`external_id`, 'Nullable(String)') AS `external_id`,
  CAST(`first_name`, 'Nullable(String)') AS `first_name`,
  CAST(`middle_name`, 'Nullable(String)') AS `middle_name`,
  CAST(`last_name`, 'Nullable(String)') AS `last_name`,
  CAST(`position`, 'Nullable(String)') AS `position`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(`role`, 'Nullable(String)') AS `role`,
  CAST(`working_schedule`, 'Nullable(String)') AS `working_schedule`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`authority_id`)), '')), 'Nullable(UUID)') AS `authority_id`
from raw_source
