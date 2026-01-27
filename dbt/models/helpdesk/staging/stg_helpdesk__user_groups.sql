{{
  config(
    materialized='view',
    alias='stg_user_groups',
    tags=['helpdesk', 'user_groups', 'staging', 'debug'],
  )
}}

with raw_source as (
  select `id`, `data`, `type`, `name`, `created_at`, `updated_at`, `deleted_at`, `entity_id`, `document_flow_name`, `document_flow`, `vat_included`, `partner_id`, `partner_relation_code`
  from {{ s3_parquet_run('helpdesk', 'user_groups', '`id` String, `data` Nullable(String), `type` Nullable(String), `name` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `entity_id` String, `document_flow_name` Nullable(String), `document_flow` Nullable(String), `vat_included` Nullable(String), `partner_id` String, `partner_relation_code` Nullable(String)') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`entity_id`)), '')), 'Nullable(UUID)') AS `entity_id`,
  CAST(`document_flow_name`, 'Nullable(String)') AS `document_flow_name`,
  CAST(multiIf(lower(trimBoth(toString(`document_flow`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`document_flow`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`document_flow`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`document_flow`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `document_flow`,
  CAST(multiIf(lower(trimBoth(toString(`vat_included`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`vat_included`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`vat_included`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`vat_included`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `vat_included`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`partner_id`)), '')), 'Nullable(UUID)') AS `partner_id`,
  CAST(`partner_relation_code`, 'Nullable(String)') AS `partner_relation_code`
from raw_source
