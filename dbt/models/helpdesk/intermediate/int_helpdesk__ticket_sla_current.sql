{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='ticket_id',
    alias='cur_ticket_sla',
    engine=ch_engine_merge_tree(),
    order_by='(created_at, ticket_id)',
    partition_by='toYYYYMM(created_at)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'operational_load', 'mart_fsd', 'current'],
  )
}}

-- depends_on: {{ ref('int_helpdesk__tickets_enriched_current') }}
-- depends_on: {{ ref('int_helpdesk__lifecycle_actions_by_ticket') }}

{# Full ticket cycle (not queue): First Touch / First Resolve / Full Resolve.
   Anchor: created_at.
   First Touch: first agent action after created_at (non-empty author, not integration bots).
   First Resolve: first Resolved (auto-Closed excluded).
   Full Resolve: last Resolved (Closed not required).
   Spam service (fixed service_id) excluded.
   *_seconds = wall; *_active_seconds = wall minus deferred-status pause until metric.
   Incremental delete+insert by ticket_id; overlap from operational_load_incremental_overlap_minutes.
   Single pass over mart_ticket_actions; resolve statuses from lifecycle_actions. #}
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
  from (
    select a.ticket_id as ticket_id
    from {{ ref('mart_helpdesk__ticket_actions') }} as a
    where a.ticket_id is not null
      and a.updated_at >= (select wm - interval {{ overlap_min }} minute from watermark)
    union distinct
    select t.id as ticket_id
    from {{ ref('int_helpdesk__tickets_enriched_current') }} as t
    where t.updated_at >= (select wm - interval {{ overlap_min }} minute from watermark)
  )
  where ticket_id is not null
),
{% endif %}

tickets as (
  select
    t.id as ticket_id,
    toInt32OrZero(toString(t.number)) as ticket_number,
    t.created_at as created_at,
    coalesce(t.service_theme, 'no service') as service_theme,
    coalesce(t.new_channel, 'no channel') as new_channel,
    coalesce(t.user_group_name, 'no group') as user_group_name,
    coalesce(t.current_status_name, '') as current_status_name
  from (
    select
      id,
      number,
      created_at,
      service_theme,
      new_channel,
      user_group_name,
      current_status_name,
      service_id,
      updated_at
    from {{ ref('int_helpdesk__tickets_enriched_current') }}
    {% if is_incremental() %}
    prewhere id in (select ticket_id from affected_tickets)
    {% endif %}
    order by id, updated_at desc
    limit 1 by id
  ) as t
  where t.service_id != toUUID('2a92c135-4afe-4c70-b651-99306a6c0f08')
),

-- Single pass: touch (all object_type) + dedup
actions_all as (
  select
    ticket_id,
    updated_at,
    user_name,
    object_type,
    status_change
  from (
    select
      a.ticket_id as ticket_id,
      a.updated_at as updated_at,
      a.user_name as user_name,
      a.object_type as object_type,
      a.status_change as status_change
    from {{ ref('mart_helpdesk__ticket_actions') }} as a
    {% if is_incremental() %}
    prewhere a.ticket_id in (select ticket_id from affected_tickets)
    where a.ticket_id is not null
      and a.updated_at >= (select min(created_at) from tickets)
    {% else %}
    where a.ticket_id is not null
    {% endif %}
    order by a.id, a.updated_at desc
    limit 1 by a.id
  )
),

touch_candidates as (
  select
    a.ticket_id as ticket_id,
    a.updated_at as touch_at
  from actions_all as a
  inner join tickets as t on t.ticket_id = a.ticket_id
  where a.updated_at > t.created_at
    and trimBoth(ifNull(a.user_name, '')) != ''
    and a.user_name not like '%ESP%'
    and a.user_name not like '%FSD%'
),

first_touch as (
  select
    ticket_id,
    nullIf(min(touch_at), toDateTime('1970-01-01 00:00:00')) as first_touch_at
  from touch_candidates
  group by ticket_id
),

-- Resolve statuses: narrow layer by ticket_id, no repeated JSON on hot path
status_events as (
  select
    a.ticket_id as ticket_id,
    a.updated_at as event_at,
    if(
      a.new_status != '',
      a.new_status,
      ifNull(JSONExtractString(toString(a.status_change), 'new_status'), '')
    ) as new_status
  from (
    select
      ticket_id,
      updated_at,
      new_status,
      status_change
    from {{ ref('int_helpdesk__lifecycle_actions_by_ticket') }}
    {% if is_incremental() %}
    prewhere ticket_id in (select ticket_id from affected_tickets)
    {% endif %}
    where object_type = 'tickets'
      and ticket_id is not null
      and status_change is not null
      {% if is_incremental() %}
      and updated_at >= (select min(created_at) from tickets)
      {% endif %}
    order by ticket_id, id, updated_at desc
    limit 1 by id
  ) as a
  inner join tickets as t on t.ticket_id = a.ticket_id
  where a.updated_at > t.created_at
    and if(
      a.new_status != '',
      a.new_status,
      ifNull(JSONExtractString(toString(a.status_change), 'new_status'), '')
    ) != ''
),

-- First = first Resolved; Full = last Resolved (Closed not required).
resolve_times as (
  select
    ticket_id,
    nullIf(minIf(event_at, new_status = 'Resolved'), toDateTime('1970-01-01 00:00:00')) as first_resolve_at,
    nullIf(maxIf(event_at, new_status = 'Resolved'), toDateTime('1970-01-01 00:00:00')) as full_resolve_at
  from status_events
  group by ticket_id
),

reopen_counts as (
  select
    l.ticket_id as ticket_id,
    countIf(l.load_category = 'reopened') as reopen_count
  from {{ ref('int_helpdesk__ticket_lifecycle_events') }} as l
  {% if is_incremental() %}
  prewhere l.ticket_id in (select ticket_id from affected_tickets)
  {% endif %}
  group by l.ticket_id
),

-- Deferred pause: same intervals as queue episodes
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

-- Pause in [created_at, end_at] for each metric anchor
pause_by_end as (
  select
    t.ticket_id as ticket_id,
    sumIf(
      greatest(
        dateDiff(
          'second',
          greatest(t.created_at, p.pause_from),
          least(ft.first_touch_at, coalesce(p.pause_to, now()))
        ),
        0
      ),
      isNotNull(ft.first_touch_at) AND ft.first_touch_at > t.created_at
        and p.pause_from < ft.first_touch_at
        and coalesce(p.pause_to, now()) > t.created_at
    ) as paused_to_first_touch,
    sumIf(
      greatest(
        dateDiff(
          'second',
          greatest(t.created_at, p.pause_from),
          least(rt.first_resolve_at, coalesce(p.pause_to, now()))
        ),
        0
      ),
      isNotNull(rt.first_resolve_at) AND rt.first_resolve_at > t.created_at
        and p.pause_from < rt.first_resolve_at
        and coalesce(p.pause_to, now()) > t.created_at
    ) as paused_to_first_resolve,
    sumIf(
      greatest(
        dateDiff(
          'second',
          greatest(t.created_at, p.pause_from),
          least(rt.full_resolve_at, coalesce(p.pause_to, now()))
        ),
        0
      ),
      isNotNull(rt.full_resolve_at) AND rt.full_resolve_at > t.created_at
        and p.pause_from < rt.full_resolve_at
        and coalesce(p.pause_to, now()) > t.created_at
    ) as paused_to_full_resolve,
    sumIf(
      greatest(
        dateDiff(
          'second',
          greatest(t.created_at, p.pause_from),
          least(now(), coalesce(p.pause_to, now()))
        ),
        0
      ),
      p.pause_from is not null
        and p.pause_from < now()
        and coalesce(p.pause_to, now()) > t.created_at
    ) as paused_to_now
  from tickets as t
  left join first_touch as ft on ft.ticket_id = t.ticket_id
  left join resolve_times as rt on rt.ticket_id = t.ticket_id
  left join pause_intervals as p on p.ticket_id = t.ticket_id
  group by t.ticket_id
)

select
  t.ticket_id as ticket_id,
  t.ticket_number as ticket_number,
  t.created_at as created_at,
  t.service_theme as service_theme,
  t.new_channel as new_channel,
  t.user_group_name as user_group_name,
  t.current_status_name as current_status_name,
  CAST(
    if(isNotNull(ft.first_touch_at) AND ft.first_touch_at > t.created_at, ft.first_touch_at, NULL),
    'Nullable(DateTime64(3))'
  ) as first_touch_at,
  CAST(
    if(isNotNull(rt.first_resolve_at) AND rt.first_resolve_at > t.created_at, rt.first_resolve_at, NULL),
    'Nullable(DateTime64(3))'
  ) as first_resolve_at,
  CAST(
    if(isNotNull(rt.full_resolve_at) AND rt.full_resolve_at > t.created_at, rt.full_resolve_at, NULL),
    'Nullable(DateTime64(3))'
  ) as full_resolve_at,
  if(
    isNotNull(ft.first_touch_at) AND ft.first_touch_at > t.created_at,
    dateDiff('second', t.created_at, ft.first_touch_at),
    CAST(NULL, 'Nullable(Int64)')
  ) as first_touch_seconds,
  if(
    isNotNull(rt.first_resolve_at) AND rt.first_resolve_at > t.created_at,
    dateDiff('second', t.created_at, rt.first_resolve_at),
    CAST(NULL, 'Nullable(Int64)')
  ) as first_resolve_seconds,
  if(
    isNotNull(rt.full_resolve_at) AND rt.full_resolve_at > t.created_at,
    dateDiff('second', t.created_at, rt.full_resolve_at),
    CAST(NULL, 'Nullable(Int64)')
  ) as full_resolve_seconds,
  if(
    isNotNull(ft.first_touch_at) AND ft.first_touch_at > t.created_at,
    greatest(
      dateDiff('second', t.created_at, ft.first_touch_at) - coalesce(pz.paused_to_first_touch, 0),
      0
    ),
    CAST(NULL, 'Nullable(Int64)')
  ) as first_touch_active_seconds,
  if(
    isNotNull(rt.first_resolve_at) AND rt.first_resolve_at > t.created_at,
    greatest(
      dateDiff('second', t.created_at, rt.first_resolve_at) - coalesce(pz.paused_to_first_resolve, 0),
      0
    ),
    CAST(NULL, 'Nullable(Int64)')
  ) as first_resolve_active_seconds,
  if(
    isNotNull(rt.full_resolve_at) AND rt.full_resolve_at > t.created_at,
    greatest(
      dateDiff('second', t.created_at, rt.full_resolve_at) - coalesce(pz.paused_to_full_resolve, 0),
      0
    ),
    CAST(NULL, 'Nullable(Int64)')
  ) as full_resolve_active_seconds,
  coalesce(pz.paused_to_now, 0) as paused_seconds_to_now,
  isNotNull(ft.first_touch_at) AND ft.first_touch_at > t.created_at as has_first_touch,
  isNotNull(rt.first_resolve_at) AND rt.first_resolve_at > t.created_at as has_first_resolve,
  isNotNull(rt.full_resolve_at) AND rt.full_resolve_at > t.created_at as has_full_resolve,
  coalesce(rc.reopen_count, 0) as reopen_count,
  coalesce(rc.reopen_count, 0) > 0 as had_reopen,
  now() as _dbt_loaded_at
from tickets as t
left join first_touch as ft on ft.ticket_id = t.ticket_id
left join resolve_times as rt on rt.ticket_id = t.ticket_id
left join reopen_counts as rc on rc.ticket_id = t.ticket_id
left join pause_by_end as pz on pz.ticket_id = t.ticket_id
