{{
  config(
    materialized='table',
    alias='mart_bot_theme_daily',
    order_by='(day, service_theme)',
    engine=ch_engine_merge_tree(),
    settings={'allow_nullable_key': 1},
    tags=['mart_bot', 'bot', 'bot'],
  )
}}

{# Coverage slice by subject; grain ticket_id → day×theme. #}
with session_handoff as (
  select distinct fsd_item_id as item_id
  from {{ ref('int_bot__ai_answer_dialogs_current') }}
  final
  where handoff_at is not null
),

ticket_flags as (
  select
    te.id as ticket_id,
    toDate(te.created_at) as day,
    coalesce(
      nullIf(trimBoth(te.service_theme), ''),
      nullIf(trimBoth(te.service_name), ''),
      'no theme'
    ) as service_theme,
    max(s.ticket_id = fi.id) as has_bot,
    max(s.is_classified = 1) as is_classified,
    max(s.is_answered = 1) as is_answered,
    max(ifNull(s.user_continued, 0) = 1) as user_continued,
    max(h.item_id = fi.id) as has_handoff,
    max(s.rating) as rating
  from {{ ref('mart_helpdesk__tickets_enriched') }} as te
  left join {{ ref('int_bot__fsd_items_current') }} as fi
    final
    on fi.fsd_id = te.id
  left join {{ ref('int_bot__ai_bot_sessions_current') }} as s
    final
    on s.ticket_id = fi.id
  left join session_handoff as h on h.item_id = fi.id
  group by te.id, day, service_theme
)

select
  day,
  service_theme,
  count() as tickets_in,
  countIf(has_bot) as bot_covered,
  countIf(is_classified) as classified,
  countIf(is_answered) as answered,
  countIf(is_answered and not user_continued and not has_handoff) as bot_resolved,
  countIf(user_continued) as continued,
  countIf(has_handoff) as handoff,
  countIf(rating is not null) as rated,
  countIf(rating is not null and rating >= 4) as rated_ge4,
  avgIf(rating, rating is not null) as avg_rating
from ticket_flags
group by day, service_theme
