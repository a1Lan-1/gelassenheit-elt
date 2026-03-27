{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_ai_bot_sessions',
    engine=ch_engine_replacing('updated_at'),
    order_by='(ticket_id)',
    unique_key=['ticket_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['bot', 'ai_bot_sessions', 'current'],
  )
}}

{# AI bot sessions: grain = ticket_id (= fsd_items.id, not FSD UUID).
   FSD: ticket_id → fsd_items.id → fsd_items.fsd_id → tickets.id. #}
with raw_source as (
  select
    `ticket_id`, `session_id`, `ticket_created_at`, `started_at`,
    `is_classified`, `classified_at`, `classified_service_id`, `reject_reason`,
    `is_routed`, `routed_at`, `routed_reason`, `in_group_id`, `out_group_id`,
    `is_answered`, `answered_at`, `user_continued`, `continued_at`,
    `outcome`, `outcome_at`, `rating`, `rating_at`, `dialogs_count`, `updated_at`
  from {{ bronze_parquet(
    'bot',
    'ai_bot_sessions',
    '`ticket_id` String, `session_id` String, `ticket_created_at` Nullable(String), `started_at` String, `is_classified` Nullable(UInt8), `classified_at` Nullable(String), `classified_service_id` Nullable(String), `reject_reason` Nullable(String), `is_routed` Nullable(UInt8), `routed_at` Nullable(String), `routed_reason` Nullable(String), `in_group_id` Nullable(String), `out_group_id` Nullable(String), `is_answered` Nullable(UInt8), `answered_at` Nullable(String), `user_continued` Nullable(UInt8), `continued_at` Nullable(String), `outcome` Nullable(String), `outcome_at` Nullable(String), `rating` Nullable(Int16), `rating_at` Nullable(String), `dialogs_count` Nullable(Int32), `updated_at` String'
  ) }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`ticket_id`)), '')), 'Nullable(UUID)')) AS `ticket_id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`session_id`)), '')), 'Nullable(UUID)')) AS `session_id`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`ticket_created_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `ticket_created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`started_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `started_at`,
  CAST(coalesce(`is_classified`, 0), 'UInt8') AS `is_classified`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`classified_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `classified_at`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`classified_service_id`)), '')), 'Nullable(UUID)') AS `classified_service_id`,
  CAST(`reject_reason`, 'Nullable(String)') AS `reject_reason`,
  CAST(coalesce(`is_routed`, 0), 'UInt8') AS `is_routed`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`routed_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `routed_at`,
  CAST(`routed_reason`, 'Nullable(String)') AS `routed_reason`,
  CAST(`in_group_id`, 'Nullable(String)') AS `in_group_id`,
  CAST(`out_group_id`, 'Nullable(String)') AS `out_group_id`,
  CAST(coalesce(`is_answered`, 0), 'UInt8') AS `is_answered`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`answered_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `answered_at`,
  CAST(coalesce(`user_continued`, 0), 'UInt8') AS `user_continued`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`continued_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `continued_at`,
  CAST(`outcome`, 'Nullable(String)') AS `outcome`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`outcome_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `outcome_at`,
  CAST(`rating`, 'Nullable(Int16)') AS `rating`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`rating_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `rating_at`,
  CAST(coalesce(`dialogs_count`, 0), 'Int32') AS `dialogs_count`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`
from raw_source
