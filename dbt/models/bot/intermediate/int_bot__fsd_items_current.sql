{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_fsd_items',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['bot', 'fsd_items', 'current'],
  )
}}

{# Mapping orch ↔ FSD: id = sessions.ticket_id / dialogs.fsd_item_id; fsd_id = tickets.id. #}
with raw_source as (
  select
    `id`, `fsd_id`, `fsd_number`, `entity_type`, `updated_at`
  from {{ bronze_parquet(
    'bot',
    'fsd_items',
    '`id` String, `fsd_id` Nullable(String), `fsd_number` Nullable(String), `entity_type` Nullable(String), `updated_at` String'
  ) }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`fsd_id`)), '')), 'Nullable(UUID)') AS `fsd_id`,
  CAST(nullIf(trimBoth(toString(`fsd_number`)), ''), 'Nullable(String)') AS `fsd_number`,
  CAST(coalesce(nullIf(trimBoth(toString(`entity_type`)), ''), ''), 'String') AS `entity_type`,
  CAST(
    coalesce(
      parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3),
      toDateTime64('1970-01-01 00:00:00', 3)
    ),
    'DateTime64(3)'
  ) AS `updated_at`
from raw_source
where toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')) is not null
