{{
  config(
    materialized='table',
    alias='mart_bot_daily',
    order_by='(day)',
    engine=ch_engine_merge_tree(),
    tags=['mart_bot', 'bot', 'bot'],
  )
}}

{# KPI of the AI bot by day: sessions + dialogues + usage; join FSD ticket_id.
   resolved = the bot answered and the user did not continue, without a handoff on the ticket.
   CH LEFT join: handoff match via equality keys only (default ≠ NULL). #}
with session_handoff as (
  select distinct fsd_item_id as ticket_id
  from {{ ref('int_bot__ai_answer_dialogs_current') }}
  final
  where handoff_at is not null
),

sessions as (
  select
    toDate(s.started_at) as day,
    count() as sessions_started,
    countIf(s.is_classified = 1) as sessions_classified,
    countIf(s.is_routed = 1) as sessions_routed,
    countIf(s.is_answered = 1) as sessions_answered,
    countIf(ifNull(s.user_continued, 0) = 1) as sessions_continued,
    countIf(
      s.is_answered = 1
      and ifNull(s.user_continued, 0) = 0
      and not (h.ticket_id = s.ticket_id)
    ) as sessions_bot_resolved,
    countIf(s.outcome is not null and s.outcome != '') as sessions_with_outcome,
    countIf(s.rating is not null) as sessions_rated,
    countIf(s.rating is not null and s.rating >= 4) as sessions_rated_ge4,
    countIf(s.rating is not null and s.rating < 4) as sessions_rated_lt4,
    avgIf(s.rating, s.rating is not null) as avg_session_rating,
    sum(s.dialogs_count) as dialogs_count_sum
  from {{ ref('int_bot__ai_bot_sessions_current') }} as s
  final
  left join session_handoff as h on h.ticket_id = s.ticket_id
  group by day
),

dialogs as (
  select
    toDate(created_at) as day,
    count() as dialogs_created,
    countIf(phase = 'closed') as dialogs_closed,
    countIf(handoff_at is not null) as dialogs_handoff,
    countIf(rating is not null) as dialogs_rated,
    countIf(rating is not null and rating >= 4) as dialogs_rated_ge4,
    avgIf(rating, rating is not null) as avg_dialog_rating,
    sum(recognize_cost_rub) as recognize_cost_rub,
    avg(rounds) as avg_rounds,
    sum(reopened_count) as reopened_count_sum
  from {{ ref('int_bot__ai_answer_dialogs_current') }}
  final
  group by day
),

usage as (
  select
    day,
    calls as usage_calls,
    done as usage_done,
    failed as usage_failed,
    est_tokens,
    est_cost_rub,
    if(latency_cnt > 0, latency_ms_sum / latency_cnt, null) as avg_latency_ms,
    calls_answer,
    calls_answer_rejected,
    cost_answer_rub,
    cost_quality_rub,
    cost_reply_rub
  from {{ ref('int_bot__ai_usage_daily_current') }}
  final
),

session_fsd_join as (
  select
    toDate(s.started_at) as day,
    countIf(t.id = s.ticket_id) as sessions_with_fsd_ticket
  from {{ ref('int_bot__ai_bot_sessions_current_v') }} as s
  left join {{ ref('int_helpdesk__tickets_current_v') }} as t
    on s.ticket_id = t.id
  group by day
),

days as (
  select day from sessions
  union distinct
  select day from dialogs
  union distinct
  select day from usage
  union distinct
  select day from session_fsd_join
)

select
  d.day as day,
  coalesce(s.sessions_started, 0) as sessions_started,
  coalesce(s.sessions_classified, 0) as sessions_classified,
  coalesce(s.sessions_routed, 0) as sessions_routed,
  coalesce(s.sessions_answered, 0) as sessions_answered,
  coalesce(s.sessions_continued, 0) as sessions_continued,
  coalesce(s.sessions_bot_resolved, 0) as sessions_bot_resolved,
  coalesce(s.sessions_with_outcome, 0) as sessions_with_outcome,
  coalesce(s.sessions_rated, 0) as sessions_rated,
  coalesce(s.sessions_rated_ge4, 0) as sessions_rated_ge4,
  coalesce(s.sessions_rated_lt4, 0) as sessions_rated_lt4,
  s.avg_session_rating as avg_session_rating,
  coalesce(s.dialogs_count_sum, 0) as dialogs_count_sum,
  coalesce(g.dialogs_created, 0) as dialogs_created,
  coalesce(g.dialogs_closed, 0) as dialogs_closed,
  coalesce(g.dialogs_handoff, 0) as dialogs_handoff,
  coalesce(g.dialogs_rated, 0) as dialogs_rated,
  coalesce(g.dialogs_rated_ge4, 0) as dialogs_rated_ge4,
  g.avg_dialog_rating as avg_dialog_rating,
  coalesce(g.recognize_cost_rub, 0) as recognize_cost_rub,
  g.avg_rounds as avg_rounds,
  coalesce(g.reopened_count_sum, 0) as reopened_count_sum,
  coalesce(u.usage_calls, 0) as usage_calls,
  coalesce(u.usage_done, 0) as usage_done,
  coalesce(u.usage_failed, 0) as usage_failed,
  coalesce(u.est_tokens, 0) as est_tokens,
  coalesce(u.est_cost_rub, 0) as est_cost_rub,
  u.avg_latency_ms as avg_latency_ms,
  coalesce(u.calls_answer, 0) as calls_answer,
  coalesce(u.calls_answer_rejected, 0) as calls_answer_rejected,
  coalesce(u.cost_answer_rub, 0) as cost_answer_rub,
  coalesce(u.cost_quality_rub, 0) as cost_quality_rub,
  coalesce(u.cost_reply_rub, 0) as cost_reply_rub,
  coalesce(f.sessions_with_fsd_ticket, 0) as sessions_with_fsd_ticket
from days as d
left join sessions as s on d.day = s.day
left join dialogs as g on d.day = g.day
left join usage as u on d.day = u.day
left join session_fsd_join as f on d.day = f.day
