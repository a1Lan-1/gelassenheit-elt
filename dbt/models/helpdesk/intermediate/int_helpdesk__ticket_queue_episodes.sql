{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='ticket_id',
    alias='cur_ticket_queue_episodes',
    engine=ch_engine_merge_tree(),
    order_by='(entry_at, ticket_id, episode_id)',
    partition_by='toYYYYMM(entry_at)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'operational_load', 'mart_fsd', 'current'],
  )
}}

{# Episode = time ticket spends on a queue.
   Start: escalated_in | reopened (formal Reopened or informal exit from terminal).
   End: completed | escalated_out (queue cleared load).
   duration_seconds = wall-clock; duration_active_seconds = wall minus deferred pause.
   BI deferral-pause filter: yes → active, no → wall.
   Spam service excluded from SLA.
   Starts without end: reentered / open.
   Incremental delete+insert by ticket_id; lifecycle watermark + overlap. #}
{% set overlap_min = var('operational_load_incremental_overlap_minutes', 10) | int %}

with
{% if is_incremental() %}
watermark as (
  select coalesce(
    max(_dbt_loaded_at),
    toDateTime('1970-01-01 00:00:00', 'Europe/Moscow')
  ) as wm
  from {{ this }}
),

affected_tickets as (
  select distinct ticket_id
  from {{ ref('int_helpdesk__ticket_lifecycle_events') }}
  where ticket_id is not null
    and _dbt_loaded_at >= (select wm - interval {{ overlap_min }} minute from watermark)
),
{% endif %}

lifecycle as (
  select
    event_id,
    ticket_id,
    ticket_number,
    event_at,
    event_source,
    load_category,
    user_group_name,
    service_theme,
    new_channel
  from {{ ref('int_helpdesk__ticket_lifecycle_events') }}
  {% if is_incremental() %}
  prewhere ticket_id in (select ticket_id from affected_tickets)
  {% endif %}
  where user_group_name != ''
    and load_category in (
      'escalated_in',
      'escalated_out',
      'completed',
      'reopened'
    )
),

-- Pause markers: deferred + terminators
pause_marks as (
  select
    ticket_id,
    event_at,
    event_id,
    load_category
  from {{ ref('int_helpdesk__ticket_lifecycle_events') }}
  {% if is_incremental() %}
  prewhere ticket_id in (select ticket_id from affected_tickets)
  {% endif %}
  where load_category in (
      'deferred',
      'resumed',
      'completed',
      'escalated_out',
      'reopened'
    )
),

pause_marks_seq as (
  select
    ticket_id,
    event_at as pause_from,
    event_id,
    load_category,
    leadInFrame(event_at) over (
      partition by ticket_id
      order by event_at, event_id
      rows between unbounded preceding and unbounded following
    ) as next_mark_at
  from pause_marks
),

pause_intervals as (
  select
    ticket_id,
    pause_from,
    next_mark_at as pause_to
  from pause_marks_seq
  where load_category = 'deferred'
),

starts as (
  select
    ticket_id,
    ticket_number,
    user_group_name,
    event_at as entry_at,
    event_id as entry_event_id,
    multiIf(
      load_category = 'reopened',
      'reopened',
      event_source = 'ticket_create',
      'ticket_create',
      'group_change'
    ) as entry_source,
    service_theme,
    new_channel
  from lifecycle
  where load_category in ('escalated_in', 'reopened')
),

ends as (
  select
    ticket_id,
    user_group_name,
    event_at as exit_at,
    event_id as exit_event_id,
    load_category as exit_reason
  from lifecycle
  where load_category in ('escalated_out', 'completed')
),

ends_ranked as (
  select
    *,
    row_number() over (
      partition by ticket_id, user_group_name
      order by exit_at, exit_event_id
    ) as end_rn
  from ends
),

end_start_pairs as (
  select
    s.ticket_id as ticket_id,
    s.ticket_number as ticket_number,
    s.user_group_name as user_group_name,
    s.entry_at as entry_at,
    s.entry_event_id as entry_event_id,
    s.entry_source as entry_source,
    s.service_theme as service_theme,
    s.new_channel as new_channel,
    e.exit_at as exit_at,
    e.exit_event_id as exit_event_id,
    e.exit_reason as exit_reason,
    e.end_rn as end_rn
  from ends_ranked as e
  asof inner join starts as s
    on e.ticket_id = s.ticket_id
    and e.user_group_name = s.user_group_name
    and e.exit_at >= s.entry_at
),

matched as (
  select
    ticket_id,
    ticket_number,
    user_group_name,
    entry_at,
    entry_event_id,
    entry_source,
    service_theme,
    new_channel,
    exit_at,
    exit_event_id,
    exit_reason
  from (
    select
      *,
      row_number() over (
        partition by ticket_id, user_group_name, entry_event_id
        order by end_rn asc, exit_at asc, exit_event_id asc
      ) as start_claim_rn
    from end_start_pairs
  )
  where start_claim_rn = 1
),

closed as (
  select
    concat(toString(ticket_id), ':', user_group_name, ':', entry_event_id) as episode_id,
    ticket_id,
    ticket_number,
    user_group_name,
    entry_at,
    entry_source,
    entry_event_id,
    exit_at,
    exit_reason,
    exit_event_id,
    dateDiff('second', entry_at, exit_at) as wall_duration_seconds,
    service_theme,
    new_channel
  from matched
),

starts_seq as (
  select
    *,
    leadInFrame(entry_at) over (
      partition by ticket_id, user_group_name
      order by entry_at, entry_event_id
      rows between unbounded preceding and unbounded following
    ) as next_entry_at,
    leadInFrame(entry_event_id) over (
      partition by ticket_id, user_group_name
      order by entry_at, entry_event_id
      rows between unbounded preceding and unbounded following
    ) as next_entry_event_id
  from starts
),

unmatched_seq as (
  select
    s.ticket_id as ticket_id,
    s.ticket_number as ticket_number,
    s.user_group_name as user_group_name,
    s.entry_at as entry_at,
    s.entry_source as entry_source,
    s.entry_event_id as entry_event_id,
    s.service_theme as service_theme,
    s.new_channel as new_channel,
    s.next_entry_at as next_entry_at,
    s.next_entry_event_id as next_entry_event_id
  from starts_seq as s
  anti join matched as m
    on m.ticket_id = s.ticket_id
    and m.user_group_name = s.user_group_name
    and m.entry_event_id = s.entry_event_id
),

reentered as (
  select
    concat(toString(ticket_id), ':', user_group_name, ':', entry_event_id) as episode_id,
    ticket_id,
    ticket_number,
    user_group_name,
    entry_at,
    entry_source,
    entry_event_id,
    next_entry_at as exit_at,
    'reentered' as exit_reason,
    CAST(NULL, 'Nullable(String)') as exit_event_id,
    greatest(dateDiff('second', entry_at, next_entry_at), 0) as wall_duration_seconds,
    service_theme,
    new_channel
  from unmatched_seq
  where next_entry_event_id != ''
),

open_eps as (
  select
    concat(toString(ticket_id), ':', user_group_name, ':', entry_event_id) as episode_id,
    ticket_id,
    ticket_number,
    user_group_name,
    entry_at,
    entry_source,
    entry_event_id,
    CAST(NULL, 'Nullable(DateTime64(3))') as exit_at,
    'open' as exit_reason,
    CAST(NULL, 'Nullable(String)') as exit_event_id,
    dateDiff('second', entry_at, now()) as wall_duration_seconds,
    service_theme,
    new_channel
  from unmatched_seq
  where next_entry_event_id = ''
),

episodes_wall as (
  select * from closed
  union all
  select * from reentered
  union all
  select * from open_eps
),

episode_pause as (
  select
    e.episode_id as episode_id,
    sum(
      greatest(
        dateDiff(
          'second',
          greatest(e.entry_at, p.pause_from),
          least(
            coalesce(e.exit_at, now()),
            coalesce(p.pause_to, now())
          )
        ),
        0
      )
    ) as paused_seconds
  from episodes_wall as e
  inner join pause_intervals as p on p.ticket_id = e.ticket_id
  where p.pause_from < coalesce(e.exit_at, now())
    and coalesce(p.pause_to, now()) > e.entry_at
  group by e.episode_id
)

select
  e.episode_id as episode_id,
  e.ticket_id as ticket_id,
  e.ticket_number as ticket_number,
  e.user_group_name as user_group_name,
  e.entry_at as entry_at,
  e.entry_source as entry_source,
  e.entry_event_id as entry_event_id,
  e.exit_at as exit_at,
  e.exit_reason as exit_reason,
  e.exit_event_id as exit_event_id,
  e.wall_duration_seconds as wall_duration_seconds,
  coalesce(p.paused_seconds, 0) as paused_seconds,
  e.wall_duration_seconds as duration_seconds,
  greatest(e.wall_duration_seconds - coalesce(p.paused_seconds, 0), 0) as duration_active_seconds,
  coalesce(e.service_theme, 'no service') as service_theme,
  coalesce(e.new_channel, 'no channel') as new_channel,
  now() as _dbt_loaded_at
from episodes_wall as e
left join episode_pause as p on p.episode_id = e.episode_id
-- Spam filter without mart FINAL: dedup via int enriched
inner join (
  select id, service_id
  from {{ ref('int_helpdesk__tickets_enriched_current') }}
  {% if is_incremental() %}
  prewhere id in (select ticket_id from affected_tickets)
  {% endif %}
  order by id, updated_at desc
  limit 1 by id
) as t on t.id = e.ticket_id
where e.wall_duration_seconds >= 0
  -- spam service (fixed service_id)
  and t.service_id != toUUID('2a92c135-4afe-4c70-b651-99306a6c0f08')
