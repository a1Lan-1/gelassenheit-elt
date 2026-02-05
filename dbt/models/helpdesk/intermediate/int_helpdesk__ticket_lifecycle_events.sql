{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='ticket_id',
    alias='cur_ticket_lifecycle_events',
    engine=ch_engine_merge_tree(),
    order_by='(event_at, ticket_id, event_id)',
    partition_by='toYYYYMM(event_at)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'operational_load', 'mart_fsd', 'current'],
  )
}}

-- depends_on: {{ ref('int_helpdesk__tickets_enriched_current') }}
-- depends_on: {{ ref('int_helpdesk__lifecycle_actions_by_ticket') }}

{# Incremental delete+insert by ticket_id — full action history for affected tickets.
   Watermark = max(_dbt_loaded_at); overlap = operational_load_incremental_overlap_minutes.
   Daily mart: operational_load_lookback_days. Full-refresh: --full-refresh on lifecycle.
   Entry: user_group_change with empty old and non-empty new (create in actions) → escalated_in
   on new_group. Before 2026-08-11 15:20 MSK dispatch remaps to L1 Support (not dual).
   Status group as-of: last create/group_change.new_group with event_at <= status time.
   Reopened: formal Reopened from terminal OR leave Resolved/Closed to non-terminal
   (informal reopen, e.g. Resolved→In Progress/Deferred).
   Deferred/resumed: Deferred|Waiting Vendor|In Development|Integrations
   (active-time pause for episodes/ticket SLA; resumed not in operational load). #}
{% set lookback_days = var('operational_load_lookback_days', 3) | int %}
{% set overlap_min = var('operational_load_incremental_overlap_minutes', 10) | int %}
{% set cutover = "toDateTime('2026-08-11 15:20:00', 'Europe/Moscow')" %}

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
    from {{ ref('int_helpdesk__lifecycle_actions_by_ticket') }} as a
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
    id,
    number,
    user_group_name,
    service_theme,
    new_channel,
    created_at
  from (
    select
      id,
      number,
      user_group_name,
      service_theme,
      new_channel,
      created_at,
      updated_at
    from {{ ref('int_helpdesk__tickets_enriched_current') }}
    {% if is_incremental() %}
    prewhere id in (select ticket_id from affected_tickets)
    {% endif %}
    order by id, updated_at desc
    limit 1 by id
  )
),

-- No FINAL: PREWHERE + LIMIT 1 BY (ticket_id, …)
-- Fallback JSON: old rows may have empty new_*/old_* before FR layer.
actions_raw as (
  select
    a.id as source_id,
    a.ticket_id as ticket_id,
    toInt32OrZero(a.ticket_number) as ticket_number,
    a.user_name as user_name,
    a.updated_at as event_at,
    a.object_type as object_type,
    if(
      a.new_status != '',
      a.new_status,
      ifNull(JSONExtractString(toString(a.status_change), 'new_status'), '')
    ) as new_status,
    if(
      a.old_status != '',
      a.old_status,
      ifNull(JSONExtractString(toString(a.status_change), 'old_status'), '')
    ) as old_status,
    if(
      a.old_group != '',
      a.old_group,
      ifNull(JSONExtractString(toString(a.user_group_change), 'old_group'), '')
    ) as old_group_raw,
    if(
      a.new_group != '',
      a.new_group,
      ifNull(JSONExtractString(toString(a.user_group_change), 'new_group'), '')
    ) as new_group_raw
  from (
    select
      id,
      ticket_id,
      ticket_number,
      user_name,
      updated_at,
      object_type,
      status_change,
      user_group_change,
      new_status,
      old_status,
      old_group,
      new_group
    from {{ ref('int_helpdesk__lifecycle_actions_by_ticket') }}
    {% if is_incremental() %}
    prewhere ticket_id in (select ticket_id from affected_tickets)
    {% endif %}
    where object_type in ('tickets')
      and ticket_id is not null
      and (status_change is not null or user_group_change is not null)
    order by ticket_id, id, updated_at desc
    limit 1 by id
  ) as a
),

actions as (
  select
    a.*,
    a.old_group_raw not in ('', 'null')
      and a.new_group_raw not in ('', 'null')
      and a.old_group_raw != a.new_group_raw as is_group_change,
    -- create / initial group assignment: empty old, non-empty new
    a.old_group_raw in ('', 'null')
      and a.new_group_raw not in ('', 'null') as is_create_entry,
    a.new_status != '' as is_status_change
  from actions_raw as a
),

-- Before cutover dispatch maps to L1 Support (rename, not duplicate row)
actions_mapped as (
  select
    a.*,
    multiIf(
      a.event_at < {{ cutover }}
        and a.old_group_raw = '00.Dispatch',
      'L1 Support',
      a.old_group_raw
    ) as old_group,
    multiIf(
      a.event_at < {{ cutover }}
        and a.new_group_raw = '00.Dispatch',
      'L1 Support',
      a.new_group_raw
    ) as new_group
  from actions as a
),

group_changes_base as (
  select a.*
  from actions_mapped as a
  where a.is_group_change
    and a.old_group != ''
    and a.new_group != ''
),

group_events_out as (
  select
    concat('group_out:', toString(a.source_id)) as event_id,
    a.source_id as source_id,
    a.ticket_id as ticket_id,
    a.ticket_number as ticket_number,
    a.user_name as user_name,
    a.event_at as event_at,
    'group_change' as event_source,
    a.old_group_raw as old_group_raw,
    a.new_group_raw as new_group_raw,
    a.old_group as old_group,
    a.new_group as new_group,
    '' as old_status,
    '' as new_status,
    'escalated_out' as load_category,
    a.old_group as user_group_name
  from group_changes_base as a
),

group_events_in as (
  select
    concat('group_in:', toString(a.source_id)) as event_id,
    a.source_id as source_id,
    a.ticket_id as ticket_id,
    a.ticket_number as ticket_number,
    a.user_name as user_name,
    a.event_at as event_at,
    'group_change' as event_source,
    a.old_group_raw as old_group_raw,
    a.new_group_raw as new_group_raw,
    a.old_group as old_group,
    a.new_group as new_group,
    '' as old_status,
    '' as new_status,
    'escalated_in' as load_category,
    a.new_group as user_group_name
  from group_changes_base as a
),

group_events as (
  select * from group_events_out
  union all
  select * from group_events_in
),

-- Entry on create group (empty old in user_group_change), not current enriched
create_events as (
  select
    concat('create:', toString(a.source_id)) as event_id,
    a.source_id as source_id,
    a.ticket_id as ticket_id,
    a.ticket_number as ticket_number,
    a.user_name as user_name,
    a.event_at as event_at,
    'ticket_create' as event_source,
    a.old_group_raw as old_group_raw,
    a.new_group_raw as new_group_raw,
    a.old_group as old_group,
    a.new_group as new_group,
    '' as old_status,
    '' as new_status,
    'escalated_in' as load_category,
    a.new_group as user_group_name
  from actions_mapped as a
  where a.is_create_entry
    and a.new_group != ''
),

-- Moments ticket enters a group (for status as-of)
group_membership_events as (
  select
    ticket_id,
    event_at,
    new_group as group_name
  from group_changes_base
  where new_group != ''

  union all

  select
    ticket_id,
    event_at,
    new_group as group_name
  from create_events
  where new_group != ''
),

status_events_base as (
  select
    concat('status:', toString(a.source_id)) as event_id,
    a.source_id as source_id,
    a.ticket_id as ticket_id,
    a.ticket_number as ticket_number,
    a.user_name as user_name,
    a.event_at as event_at,
    'status_action' as event_source,
    a.old_group_raw as old_group_raw,
    a.new_group_raw as new_group_raw,
    a.old_group as old_group,
    a.new_group as new_group,
    a.old_status as old_status,
    a.new_status as new_status,
    -- completed / reopen (incl. informal); deferred/resumed as separate rows below
    multiIf(
      a.new_status = 'Resolved',
      'completed',
      a.new_status = 'Closed' and a.old_status != 'Resolved',
      'completed',
      -- formal reopen
      a.new_status = 'Reopened' and a.old_status in ('Resolved', 'Closed'),
      'reopened',
      -- informal: left terminal to non-terminal
      -- Resolved→Closed excluded (new is terminal)
      a.old_status in ('Resolved', 'Closed')
        and a.new_status not in ('Resolved', 'Closed', ''),
      'reopened',
      'other'
    ) as load_category,
    multiIf(
      a.event_at < {{ cutover }}
        and t.user_group_name = '00.Dispatch',
      'L1 Support',
      t.user_group_name
    ) as enriched_group
  from actions_mapped as a
  inner join tickets as t on t.id = a.ticket_id
  where a.is_status_change
),

status_asof as (
  select
    b.event_id as event_id,
    m.group_name as asof_group
  from status_events_base as b
  asof left join group_membership_events as m
    on b.ticket_id = m.ticket_id
    and b.event_at >= m.event_at
),

status_events_raw as (
  select
    b.event_id as event_id,
    b.source_id as source_id,
    b.ticket_id as ticket_id,
    b.ticket_number as ticket_number,
    b.user_name as user_name,
    b.event_at as event_at,
    b.event_source as event_source,
    b.old_group_raw as old_group_raw,
    b.new_group_raw as new_group_raw,
    b.old_group as old_group,
    b.new_group as new_group,
    b.old_status as old_status,
    b.new_status as new_status,
    b.load_category as load_category,
    coalesce(nullIf(a.asof_group, ''), b.enriched_group) as user_group_name
  from status_events_base as b
  left join status_asof as a on a.event_id = b.event_id
),

group_times_by_ticket as (
  select
    ticket_id,
    groupArray(event_at) as group_at_arr
  from group_changes_base
  group by ticket_id
),

-- Dedup group change ±60s; exclude New ticket status
status_events_filtered as (
  select s.*
  from status_events_raw as s
  left join group_times_by_ticket as gt on gt.ticket_id = s.ticket_id
  where (
    gt.ticket_id is null
    or not arrayExists(
      x -> abs(dateDiff('second', s.event_at, x)) < 60,
      gt.group_at_arr
    )
  )
    and s.new_status != 'New ticket'
),

status_events as (
  select *
  from status_events_filtered
  where load_category in ('completed', 'reopened')
),

-- Active-time pause (optional BI: wall vs active)
{% set pause_statuses = "('Deferred', 'Waiting Vendor', 'In Development', 'Integrations')" %}

status_deferred as (
  select
    concat('status_deferred:', toString(s.source_id)) as event_id,
    s.source_id as source_id,
    s.ticket_id as ticket_id,
    s.ticket_number as ticket_number,
    s.user_name as user_name,
    s.event_at as event_at,
    s.event_source as event_source,
    s.old_group_raw as old_group_raw,
    s.new_group_raw as new_group_raw,
    s.old_group as old_group,
    s.new_group as new_group,
    s.old_status as old_status,
    s.new_status as new_status,
    'deferred' as load_category,
    s.user_group_name as user_group_name
  from status_events_filtered as s
  where s.new_status in {{ pause_statuses }}
),

status_resumed as (
  select
    concat('status_resumed:', toString(s.source_id)) as event_id,
    s.source_id as source_id,
    s.ticket_id as ticket_id,
    s.ticket_number as ticket_number,
    s.user_name as user_name,
    s.event_at as event_at,
    s.event_source as event_source,
    s.old_group_raw as old_group_raw,
    s.new_group_raw as new_group_raw,
    s.old_group as old_group,
    s.new_group as new_group,
    s.old_status as old_status,
    s.new_status as new_status,
    'resumed' as load_category,
    s.user_group_name as user_group_name
  from status_events_filtered as s
  where s.old_status in {{ pause_statuses }}
    and s.new_status not in {{ pause_statuses }}
    and s.new_status != ''
),

combined as (
  select * from group_events
  union all
  select * from create_events
  union all
  select * from status_events
  union all
  select * from status_deferred
  union all
  select * from status_resumed
)

select
  c.event_id as event_id,
  c.ticket_id as ticket_id,
  c.ticket_number as ticket_number,
  c.event_at as event_at,
  c.event_source as event_source,
  c.old_group_raw as old_group_raw,
  c.new_group_raw as new_group_raw,
  c.old_group as old_group,
  c.new_group as new_group,
  c.old_status as old_status,
  c.new_status as new_status,
  c.load_category as load_category,
  c.user_group_name as user_group_name,
  coalesce(t.service_theme, 'no service') as service_theme,
  coalesce(t.new_channel, 'no channel') as new_channel,
  c.user_name as user_name,
  now() as _dbt_loaded_at
from combined as c
left join tickets as t on t.id = c.ticket_id
where c.load_category != 'other'
  and c.user_group_name != ''
