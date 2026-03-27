{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_ai_answer_dialogs',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['bot', 'ai_answer_dialogs', 'current'],
  )
}}

{# Bot response dialogs: grain = id; fsd_item_id = fsd_items.id (not FSD UUID). #}
with raw_source as (
  select
    `id`, `fsd_item_id`, `fsd_number`, `card_id`, `last_job_id`,
    `phase`, `phase_entered_at`, `next_action_at`, `channel`, `rounds`,
    `warning_sent_at`, `rating`, `rating_source`, `rated_at`,
    `replies_total`, `replies_unrecognized`,
    `close_reason`, `closure_code`, `closed_at`, `close_attempts`,
    `handoff_reason`, `handoff_at`, `handoff_group_id`, `error_text`,
    `version`, `created_at`, `updated_at`,
    `recognize_cost_rub`, `reopened_count`, `activity_calls`, `rounds_new_question`
  from {{ bronze_parquet(
    'bot',
    'ai_answer_dialogs',
    '`id` String, `fsd_item_id` String, `fsd_number` Nullable(String), `card_id` Nullable(String), `last_job_id` Nullable(String), `phase` String, `phase_entered_at` String, `next_action_at` Nullable(String), `channel` String, `rounds` Nullable(Int32), `warning_sent_at` Nullable(String), `rating` Nullable(Int16), `rating_source` Nullable(String), `rated_at` Nullable(String), `replies_total` Nullable(Int32), `replies_unrecognized` Nullable(Int32), `close_reason` Nullable(String), `closure_code` Nullable(String), `closed_at` Nullable(String), `close_attempts` Nullable(Int32), `handoff_reason` Nullable(String), `handoff_at` Nullable(String), `handoff_group_id` Nullable(String), `error_text` Nullable(String), `version` Nullable(Int32), `created_at` String, `updated_at` String, `recognize_cost_rub` Nullable(Float64), `reopened_count` Nullable(Int32), `activity_calls` Nullable(Int32), `rounds_new_question` Nullable(Int32)'
  ) }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`fsd_item_id`)), '')), 'Nullable(UUID)')) AS `fsd_item_id`,
  CAST(`fsd_number`, 'Nullable(String)') AS `fsd_number`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`card_id`)), '')), 'Nullable(UUID)') AS `card_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`last_job_id`)), '')), 'Nullable(UUID)') AS `last_job_id`,
  CAST(`phase`, 'String') AS `phase`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`phase_entered_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `phase_entered_at`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`next_action_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `next_action_at`,
  CAST(`channel`, 'String') AS `channel`,
  CAST(coalesce(`rounds`, 0), 'Int32') AS `rounds`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`warning_sent_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `warning_sent_at`,
  CAST(`rating`, 'Nullable(Int16)') AS `rating`,
  CAST(`rating_source`, 'Nullable(String)') AS `rating_source`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`rated_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `rated_at`,
  CAST(coalesce(`replies_total`, 0), 'Int32') AS `replies_total`,
  CAST(coalesce(`replies_unrecognized`, 0), 'Int32') AS `replies_unrecognized`,
  CAST(`close_reason`, 'Nullable(String)') AS `close_reason`,
  CAST(`closure_code`, 'Nullable(String)') AS `closure_code`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closed_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `closed_at`,
  CAST(coalesce(`close_attempts`, 0), 'Int32') AS `close_attempts`,
  CAST(`handoff_reason`, 'Nullable(String)') AS `handoff_reason`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`handoff_at`)), ''), 3), 'Nullable(DateTime64(3))') AS `handoff_at`,
  CAST(`handoff_group_id`, 'Nullable(String)') AS `handoff_group_id`,
  CAST(`error_text`, 'Nullable(String)') AS `error_text`,
  CAST(coalesce(`version`, 0), 'Int32') AS `version`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST(coalesce(`recognize_cost_rub`, 0), 'Float64') AS `recognize_cost_rub`,
  CAST(coalesce(`reopened_count`, 0), 'Int32') AS `reopened_count`,
  CAST(coalesce(`activity_calls`, 0), 'Int32') AS `activity_calls`,
  CAST(coalesce(`rounds_new_question`, 0), 'Int32') AS `rounds_new_question`
from raw_source
