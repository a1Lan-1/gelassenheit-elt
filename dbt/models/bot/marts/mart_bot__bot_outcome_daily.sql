{{
  config(
    materialized='table',
    alias='mart_bot_outcome_daily',
    order_by='(day, outcome)',
    engine=ch_engine_merge_tree(),
    settings={'allow_nullable_key': 1},
    tags=['mart_bot', 'bot', 'bot'],
  )
}}

{# Outcomes of bot sessions by day (outcome as-is + empty → "without outcome"). #}
select
  toDate(started_at) as day,
  coalesce(nullIf(trimBoth(outcome), ''), 'no outcome') as outcome,
  count() as sessions_started,
  countIf(is_answered = 1) as sessions_answered,
  countIf(user_continued = 1) as sessions_continued,
  countIf(is_answered = 1 and user_continued = 0) as sessions_answered_not_continued,
  countIf(rating is not null) as sessions_rated,
  avgIf(rating, rating is not null) as avg_session_rating
from {{ ref('int_bot__ai_bot_sessions_current') }}
final
group by day, outcome
