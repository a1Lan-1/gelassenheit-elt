{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_entities',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'entities', 'current'],
  )
}}

with raw_source as (
  select `id`, `external_id`, `parent_id`, `prefix`, `name`, `short_name`, `legal_address`, `actual_address`, `inn`, `kpp`, `ogrn`, `okpo`, `okato`, `certificate_number`, `created_at`, `updated_at`, `deleted_at`, `type`, `comment`, `type_id`
  from {{ bronze_parquet('helpdesk', 'entities', '`id` String, `external_id` Nullable(String), `parent_id` String, `prefix` Nullable(String), `name` Nullable(String), `short_name` Nullable(String), `legal_address` Nullable(String), `actual_address` Nullable(String), `inn` Nullable(String), `kpp` Nullable(String), `ogrn` Nullable(String), `okpo` Nullable(String), `okato` Nullable(String), `certificate_number` Nullable(String), `created_at` String, `updated_at` String, `deleted_at` String, `type` Nullable(String), `comment` Nullable(String), `type_id` String') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(`external_id`, 'Nullable(String)') AS `external_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`parent_id`)), '')), 'Nullable(UUID)') AS `parent_id`,
  CAST(`prefix`, 'Nullable(String)') AS `prefix`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(`short_name`, 'Nullable(String)') AS `short_name`,
  CAST(`legal_address`, 'Nullable(String)') AS `legal_address`,
  CAST(`actual_address`, 'Nullable(String)') AS `actual_address`,
  CAST(`inn`, 'Nullable(String)') AS `inn`,
  CAST(`kpp`, 'Nullable(String)') AS `kpp`,
  CAST(`ogrn`, 'Nullable(String)') AS `ogrn`,
  CAST(`okpo`, 'Nullable(String)') AS `okpo`,
  CAST(`okato`, 'Nullable(String)') AS `okato`,
  CAST(`certificate_number`, 'Nullable(String)') AS `certificate_number`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(`type`, 'Nullable(String)') AS `type`,
  CAST(`comment`, 'Nullable(String)') AS `comment`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`type_id`)), '')), 'Nullable(UUID)') AS `type_id`
from raw_source