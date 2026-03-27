{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_ai_usage_daily',
    engine=ch_engine_replacing('_loaded_at'),
    order_by='(day)',
    unique_key=['day'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['bot', 'ai_usage_daily', 'current'],
  )
}}

{# Daily usage aggregates; full snapshot from bronze — version = _loaded_at. #}
with raw_source as (
  select
    `day`, `calls`, `done`, `failed`,
    `skipped_input_unchanged`, `skipped_filtered_status`, `skipped_max_reached`,
    `skipped_other`, `skipped_not_in_scope`, `skipped_input_filtered`, `marker_missing`,
    `calls_live`, `calls_backfill`, `calls_reopened`, `calls_test_run`, `calls_manual`,
    `calls_answer`, `calls_answer_rejected`, `calls_answer_free`, `calls_quality_check`,
    `calls_reply_recognize`, `calls_classic_ml`, `classic_ml_failed`,
    `est_tokens`, `est_cost_rub`, `latency_ms_sum`, `latency_cnt`,
    `tokens_answer_prompt`, `tokens_answer_completion`,
    `tokens_quality_prompt`, `tokens_quality_completion`,
    `tokens_reply_prompt`, `tokens_reply_completion`,
    `cost_answer_rub`, `cost_quality_rub`, `cost_reply_rub`,
    `classic_ml_latency_ms_sum`, `classic_ml_latency_cnt`
  from {{ bronze_parquet(
    'bot',
    'ai_usage_daily',
    '`day` String, `calls` Nullable(Int32), `done` Nullable(Int32), `failed` Nullable(Int32), `skipped_input_unchanged` Nullable(Int32), `skipped_filtered_status` Nullable(Int32), `skipped_max_reached` Nullable(Int32), `skipped_other` Nullable(Int32), `skipped_not_in_scope` Nullable(Int32), `calls_live` Nullable(Int32), `calls_backfill` Nullable(Int32), `calls_reopened` Nullable(Int32), `calls_test_run` Nullable(Int32), `est_tokens` Nullable(Int64), `est_cost_rub` Nullable(Float64), `latency_ms_sum` Nullable(Int64), `latency_cnt` Nullable(Int32), `skipped_input_filtered` Nullable(Int32), `marker_missing` Nullable(Int32), `calls_manual` Nullable(Int32), `calls_answer` Nullable(Int32), `calls_answer_rejected` Nullable(Int32), `calls_answer_free` Nullable(Int32), `calls_quality_check` Nullable(Int32), `tokens_answer_prompt` Nullable(Int64), `tokens_answer_completion` Nullable(Int64), `tokens_quality_prompt` Nullable(Int64), `tokens_quality_completion` Nullable(Int64), `cost_answer_rub` Nullable(Float64), `cost_quality_rub` Nullable(Float64), `calls_reply_recognize` Nullable(Int32), `tokens_reply_prompt` Nullable(Int64), `tokens_reply_completion` Nullable(Int64), `cost_reply_rub` Nullable(Float64), `calls_classic_ml` Nullable(Int32), `classic_ml_failed` Nullable(Int32), `classic_ml_latency_ms_sum` Nullable(Int64), `classic_ml_latency_cnt` Nullable(Int32)'
  ) }}
)
select
  assumeNotNull(CAST(toDateOrNull(nullIf(trimBoth(toString(`day`)), '')), 'Nullable(Date)')) AS `day`,
  CAST(coalesce(`calls`, 0), 'Int32') AS `calls`,
  CAST(coalesce(`done`, 0), 'Int32') AS `done`,
  CAST(coalesce(`failed`, 0), 'Int32') AS `failed`,
  CAST(coalesce(`skipped_input_unchanged`, 0), 'Int32') AS `skipped_input_unchanged`,
  CAST(coalesce(`skipped_filtered_status`, 0), 'Int32') AS `skipped_filtered_status`,
  CAST(coalesce(`skipped_max_reached`, 0), 'Int32') AS `skipped_max_reached`,
  CAST(coalesce(`skipped_other`, 0), 'Int32') AS `skipped_other`,
  CAST(coalesce(`skipped_not_in_scope`, 0), 'Int32') AS `skipped_not_in_scope`,
  CAST(coalesce(`calls_live`, 0), 'Int32') AS `calls_live`,
  CAST(coalesce(`calls_backfill`, 0), 'Int32') AS `calls_backfill`,
  CAST(coalesce(`calls_reopened`, 0), 'Int32') AS `calls_reopened`,
  CAST(coalesce(`calls_test_run`, 0), 'Int32') AS `calls_test_run`,
  CAST(coalesce(`est_tokens`, 0), 'Int64') AS `est_tokens`,
  CAST(coalesce(`est_cost_rub`, 0), 'Float64') AS `est_cost_rub`,
  CAST(coalesce(`latency_ms_sum`, 0), 'Int64') AS `latency_ms_sum`,
  CAST(coalesce(`latency_cnt`, 0), 'Int32') AS `latency_cnt`,
  CAST(coalesce(`skipped_input_filtered`, 0), 'Int32') AS `skipped_input_filtered`,
  CAST(coalesce(`marker_missing`, 0), 'Int32') AS `marker_missing`,
  CAST(coalesce(`calls_manual`, 0), 'Int32') AS `calls_manual`,
  CAST(coalesce(`calls_answer`, 0), 'Int32') AS `calls_answer`,
  CAST(coalesce(`calls_answer_rejected`, 0), 'Int32') AS `calls_answer_rejected`,
  CAST(coalesce(`calls_answer_free`, 0), 'Int32') AS `calls_answer_free`,
  CAST(coalesce(`calls_quality_check`, 0), 'Int32') AS `calls_quality_check`,
  CAST(coalesce(`tokens_answer_prompt`, 0), 'Int64') AS `tokens_answer_prompt`,
  CAST(coalesce(`tokens_answer_completion`, 0), 'Int64') AS `tokens_answer_completion`,
  CAST(coalesce(`tokens_quality_prompt`, 0), 'Int64') AS `tokens_quality_prompt`,
  CAST(coalesce(`tokens_quality_completion`, 0), 'Int64') AS `tokens_quality_completion`,
  CAST(coalesce(`cost_answer_rub`, 0), 'Float64') AS `cost_answer_rub`,
  CAST(coalesce(`cost_quality_rub`, 0), 'Float64') AS `cost_quality_rub`,
  CAST(coalesce(`calls_reply_recognize`, 0), 'Int32') AS `calls_reply_recognize`,
  CAST(coalesce(`tokens_reply_prompt`, 0), 'Int64') AS `tokens_reply_prompt`,
  CAST(coalesce(`tokens_reply_completion`, 0), 'Int64') AS `tokens_reply_completion`,
  CAST(coalesce(`cost_reply_rub`, 0), 'Float64') AS `cost_reply_rub`,
  CAST(coalesce(`calls_classic_ml`, 0), 'Int32') AS `calls_classic_ml`,
  CAST(coalesce(`classic_ml_failed`, 0), 'Int32') AS `classic_ml_failed`,
  CAST(coalesce(`classic_ml_latency_ms_sum`, 0), 'Int64') AS `classic_ml_latency_ms_sum`,
  CAST(coalesce(`classic_ml_latency_cnt`, 0), 'Int32') AS `classic_ml_latency_cnt`,
  now64(3) AS `_loaded_at`
from raw_source
