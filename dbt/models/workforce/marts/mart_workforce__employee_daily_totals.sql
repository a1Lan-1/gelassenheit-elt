{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='activity_date',
    alias='mart_employee_daily_totals',
    engine=ch_engine_merge_tree(),
    order_by='(activity_date, organization, employee_key)',
    partition_by='toYYYYMM(activity_date)',
    settings={'allow_nullable_key': 1},
    tags=['mart_gs', 'gs', 'employee_daily_totals', 'support_kpi_source'],
  )
}}

{# delete+insert by activity_date; lookback 14d (full history on FR). #}
{% set lookback_days = var('employee_daily_totals_lookback_days', 3) | int %}
{% set call_min_sec = var('employee_daily_call_min_sec', 20) | int %}
{# false → *_sk = 0 (temporary run without dialer). #}
{% set include_dialer = var('employee_daily_include_dialer', true) %}
{% if is_incremental() %}
  {% set use_lookback = true %}
{% else %}
  {% set use_lookback = false %}
{% endif %}

with employees as (
  select
    organization,
    employee_key,
    full_name,
    trimBoth(full_name) as employee_name_key,
    email,
    group_name,
    supervisor,
    employment_status
  from {{ ref('mart_workforce__employees') }}
  where not is_dismissed
    and full_name != ''
    and full_name is not null
),

schedule_esp as (
  select
    toDate(`date`) as activity_date,
    trimBoth(employee) as employee_name_key,
    sum(coalesce(hours, 0)) as scheduled_hours
  from {{ ref('int_workforce__esp_techsup_schedule_current_v') }}
  where `date` is not null and employee != '' and employee is not null
  {% if use_lookback %}
    and toDate(`date`) >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key
),

schedule_akc as (
  select
    date as activity_date,
    trimBoth(user_name) as employee_name_key,
    sum(coalesce(work_hours, 0)) as scheduled_hours
  from {{ ref('int_workforce__akc2_techsup_schedule_current_v') }}
  where date is not null and user_name != '' and user_name is not null
  {% if use_lookback %}
    and date >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key

  union all

  select
    date as activity_date,
    trimBoth(employee) as employee_name_key,
    sum(coalesce(hours, 0)) as scheduled_hours
  from {{ ref('int_workforce__akc1_techsup_schedule_current_v') }}
  where date is not null and employee != '' and employee is not null
  {% if use_lookback %}
    and date >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key
),

schedule_akc_daily as (
  select
    activity_date,
    employee_name_key,
    sum(scheduled_hours) as scheduled_hours
  from schedule_akc
  group by activity_date, employee_name_key
),

-- calltrafic temporarily excluded: Nextcloud 403 from Airflow egress
employee_days as (
  select
    e.organization as organization,
    e.employee_key as employee_key,
    e.full_name as full_name,
    e.employee_name_key as employee_name_key,
    e.email as email,
    e.group_name as group_name,
    e.supervisor as supervisor,
    e.employment_status as employment_status,
    s.activity_date as activity_date,
    s.scheduled_hours as scheduled_hours
  from schedule_esp as s
  inner join employees as e
    on e.organization = 'esp'
    and s.employee_name_key = e.employee_name_key

  union all

  select
    e.organization as organization,
    e.employee_key as employee_key,
    e.full_name as full_name,
    e.employee_name_key as employee_name_key,
    e.email as email,
    e.group_name as group_name,
    e.supervisor as supervisor,
    e.employment_status as employment_status,
    s.activity_date as activity_date,
    s.scheduled_hours as scheduled_hours
  from schedule_akc_daily as s
  inner join employees as e
    on e.organization = 'akc'
    and s.employee_name_key = e.employee_name_key
),

bounds as (
  select
    {% if use_lookback %}
    greatest(min(activity_date), today() - {{ lookback_days }}) as min_activity_date,
    least(max(activity_date), today()) as max_activity_date,
    toDateTime64(greatest(min(activity_date), today() - {{ lookback_days }}), 3) as min_ts,
    toDateTime64(least(max(activity_date), today()) + 1, 3) as max_ts_exclusive
    {% else %}
    min(activity_date) as min_activity_date,
    max(activity_date) as max_activity_date,
    toDateTime64(min(activity_date), 3) as min_ts,
    toDateTime64(max(activity_date) + 1, 3) as max_ts_exclusive
    {% endif %}
  from employee_days
),

indiv_tasks_daily as (
  select
    toDate(dt) as activity_date,
    trimBoth(user_name) as employee_name_key,
    sum(coalesce(count_hours, 0)) as indiv_hours
  from {{ ref('int_workforce__esp_techsup_activities_current_v') }}
  where activity_type = 'individual assignment'
    and dt is not null
    and user_name != ''
    and user_name is not null
  {% if use_lookback %}
    and toDate(dt) >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key
),

downtime_daily as (
  select
    toDate(dt) as activity_date,
    trimBoth(user_name) as employee_name_key,
    sum(coalesce(count_hours, 0)) as idle_hours
  from {{ ref('int_workforce__esp_techsup_activities_current_v') }}
  where activity_type = 'Idle'
    and dt is not null
    and user_name != ''
    and user_name is not null
  {% if use_lookback %}
    and toDate(dt) >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key
),

-- Single scan of ticket_actions for closures / answers / calls.
ticket_actions_scoped as (
  select
    ticket_id,
    updated_at,
    user_name as employee_name_key,
    object_type,
    appeal_info,
    comment_text,
    notify_user,
    comment_status,
    call_duration,
    -- JSON path only on table columns (alias.col.key fails on CH 26.2).
    ifNull(toString(status_change.new_status), '') as status_new
  from {{ ref('mart_helpdesk__ticket_actions') }}
  where user_name is not null
    and user_name != ''
    and updated_at >= (select min_ts from bounds)
    and updated_at < (select max_ts_exclusive from bounds)
),

closures_daily as (
  select
    toDate(updated_at) as activity_date,
    employee_name_key,
    count(*) as closed_tickets_count
  from ticket_actions_scoped
  where object_type = 'tickets'
    and status_new != ''
    and (
      positionCaseInsensitive(status_new, 'Closed') > 0
      or status_new = 'Resolved'
    )
  group by activity_date, employee_name_key
),

answer_events as (
  select
    ticket_id,
    updated_at,
    employee_name_key
  from ticket_actions_scoped
  where appeal_info like '%outgoing - email%'
    or (
      (comment_text is not null and (notify_user not like '%ESP Integration%' or notify_user is null))
      or comment_status is not null
    )
    or (comment_text is not null and notify_user like '%ESP Integration%')
),

valid_answer_rows as (
  select
    ticket_id,
    updated_at,
    employee_name_key
  from (
    select
      ticket_id,
      updated_at,
      employee_name_key,
      row_number() over (
        partition by ticket_id
        order by updated_at
        rows between unbounded preceding and unbounded following
      ) as rn,
      lagInFrame(updated_at, 1) over (
        partition by ticket_id
        order by updated_at
        rows between unbounded preceding and current row
      ) as prev_updated_at
    from answer_events
  )
  where rn = 1
    or dateDiff('minute', prev_updated_at, updated_at) >= 5
),

valid_answers as (
  select
    toDate(updated_at) as activity_date,
    employee_name_key,
    count(*) as total_answers_count
  from valid_answer_rows
  group by activity_date, employee_name_key
),

ticket_calls_daily as (
  select
    toDate(updated_at) as activity_date,
    employee_name_key,
    countIf(appeal_info like '%outgoing - phone%') as outgoing_calls_ticket,
    countIf(appeal_info like '%incoming - phone%') as incoming_calls_ticket,
    countIf(appeal_info like '%transferred - phone%') as transferred_calls_ticket,
    sum(coalesce(call_duration, 0)) as total_call_duration_ticket
  from ticket_actions_scoped
  group by activity_date, employee_name_key
),

dialer_daily as (
  {% if include_dialer %}
  select
    toDate(started_at) as activity_date,
    trimBoth(user_name) as employee_name_key,
    countIf(duration >= {{ call_min_sec }} and call_type_code = 'outgoing') as outgoing_calls_sk,
    countIf(duration >= {{ call_min_sec }} and call_type_code = 'incoming') as incoming_calls_sk,
    countIf(duration >= {{ call_min_sec }} and call_type_code = 'transfered') as transferred_calls_sk,
    sumIf(duration, duration >= {{ call_min_sec }}) as total_call_duration_sk,
    sumIf(
      coalesce(waiting_on_line_time, 0),
      duration >= {{ call_min_sec }} and call_type_code in ('incoming', 'transfered')
    ) as total_waiting_time_sk
  from {{ ref('int_dialer__calls_detail') }}
  where started_at is not null
    and user_name is not null
    and user_name != ''
    and started_at >= (select min_ts from bounds)
    and started_at < (select max_ts_exclusive from bounds)
  group by activity_date, employee_name_key
  {% else %}
  select
    toDate('1970-01-01') as activity_date,
    '' as employee_name_key,
    toUInt64(0) as outgoing_calls_sk,
    toUInt64(0) as incoming_calls_sk,
    toUInt64(0) as transferred_calls_sk,
    toInt64(0) as total_call_duration_sk,
    toInt64(0) as total_waiting_time_sk
  where 0
  {% endif %}
),

-- PBX: talk threshold (aht_talk); duration numerator for counted types only.
pbx_daily as (
  select
    toDate(started_at) as activity_date,
    trimBoth(user_name) as employee_name_key,
    countIf(
      coalesce(aht_talk_duration, talk_duration, 0) >= {{ call_min_sec }}
      and call_type_code = 'outgoing'
    ) as outgoing_calls_mg,
    countIf(
      coalesce(aht_talk_duration, talk_duration, 0) >= {{ call_min_sec }}
      and call_type_code = 'incoming'
    ) as incoming_calls_mg,
    countIf(
      coalesce(aht_talk_duration, talk_duration, 0) >= {{ call_min_sec }}
      and is_real_transfer_hop
      and call_type_code = 'transfered'
    ) as transferred_calls_mg,
    sumIf(
      coalesce(aht_talk_duration, talk_duration, 0),
      coalesce(aht_talk_duration, talk_duration, 0) >= {{ call_min_sec }}
      and (
        call_type_code in ('outgoing', 'incoming')
        or (is_real_transfer_hop and call_type_code = 'transfered')
      )
    ) as total_call_duration_mg,
    sumIf(
      coalesce(wait_to_accept_sec, 0),
      coalesce(aht_talk_duration, talk_duration, 0) >= {{ call_min_sec }}
      and (
        call_type_code = 'incoming'
        or (is_real_transfer_hop and call_type_code = 'transfered')
      )
    ) as total_waiting_time_mg
  from {{ ref('int_pbx__calls_detail') }}
  where started_at is not null
    and user_name is not null
    and user_name != ''
    and call_type_code != 'missed'
    and started_at >= (select min_ts from bounds)
    and started_at < (select max_ts_exclusive from bounds)
  group by activity_date, employee_name_key
),

qa_daily as (
  select
    date as activity_date,
    trimBoth(employee) as employee_name_key,
    sum(coalesce(score, 0)) as qa_score_sum,
    count(*) as qa_reviews_count
  from {{ ref('int_workforce__employee_audit_tickets_sec_current_v') }}
  where date is not null
    and employee != ''
    and employee is not null
  {% if use_lookback %}
    and date >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key
),

qa_only_days as (
  select
    e.organization as organization,
    e.employee_key as employee_key,
    e.full_name as full_name,
    e.employee_name_key as employee_name_key,
    e.email as email,
    e.group_name as group_name,
    e.supervisor as supervisor,
    e.employment_status as employment_status,
    qa.activity_date as activity_date,
    0 as scheduled_hours
  from qa_daily as qa
  inner join employees as e
    on qa.employee_name_key = e.employee_name_key
  left any join employee_days as ed
    on ed.organization = e.organization
    and ed.employee_key = e.employee_key
    and ed.activity_date = qa.activity_date
  where ed.employee_key is null
),

test_daily as (
  select
    toDate(dt) as activity_date,
    trimBoth(name) as employee_name_key,
    sum(coalesce(result, 0) * coalesce(n_questions, 0)) as test_weighted_sum,
    sum(coalesce(n_questions, 0)) as test_questions_sum,
    count(*) as test_attempts_count
  from {{ ref('int_workforce__esp_test_results_current_v') }}
  where dt is not null
    and name != ''
    and name is not null
  {% if use_lookback %}
    and toDate(dt) >= today() - {{ lookback_days }}
  {% endif %}
  group by activity_date, employee_name_key
),

test_only_days as (
  select
    e.organization as organization,
    e.employee_key as employee_key,
    e.full_name as full_name,
    e.employee_name_key as employee_name_key,
    e.email as email,
    e.group_name as group_name,
    e.supervisor as supervisor,
    e.employment_status as employment_status,
    td.activity_date as activity_date,
    0 as scheduled_hours
  from test_daily as td
  inner join employees as e
    on td.employee_name_key = e.employee_name_key
  left any join employee_days as ed
    on ed.organization = e.organization
    and ed.employee_key = e.employee_key
    and ed.activity_date = td.activity_date
  left any join qa_only_days as qod
    on qod.organization = e.organization
    and qod.employee_key = e.employee_key
    and qod.activity_date = td.activity_date
  where ed.employee_key is null
    and qod.employee_key is null
),

employee_days_all as (
  select * from employee_days
  union all
  select * from qa_only_days
  union all
  select * from test_only_days
)

select
  ed.activity_date as activity_date,
  ed.organization as organization,
  ed.employee_key as employee_key,
  ed.full_name as full_name,
  ed.email as email,
  ed.group_name as group_name,
  ed.supervisor as supervisor,
  ed.employment_status as employment_status,
  greatest(
    0,
    ed.scheduled_hours - coalesce(it.indiv_hours, 0) - coalesce(idle.idle_hours, 0)
  ) as working_hours,
  coalesce(cl.closed_tickets_count, 0) as closed_tickets_count,
  coalesce(va.total_answers_count, 0) as total_answers_count,
  coalesce(tc.outgoing_calls_ticket, 0) as outgoing_calls_ticket,
  coalesce(tc.incoming_calls_ticket, 0) as incoming_calls_ticket,
  coalesce(tc.transferred_calls_ticket, 0) as transferred_calls_ticket,
  coalesce(tc.total_call_duration_ticket, 0) as total_call_duration_ticket,
  coalesce(sz.outgoing_calls_sk, 0) as outgoing_calls_sk,
  coalesce(sz.incoming_calls_sk, 0) as incoming_calls_sk,
  coalesce(sz.transferred_calls_sk, 0) as transferred_calls_sk,
  coalesce(sz.total_call_duration_sk, 0) as total_call_duration_sk,
  coalesce(sz.total_waiting_time_sk, 0) as total_waiting_time_sk,
  coalesce(mg.outgoing_calls_mg, 0) as outgoing_calls_mg,
  coalesce(mg.incoming_calls_mg, 0) as incoming_calls_mg,
  coalesce(mg.transferred_calls_mg, 0) as transferred_calls_mg,
  coalesce(mg.total_call_duration_mg, 0) as total_call_duration_mg,
  coalesce(mg.total_waiting_time_mg, 0) as total_waiting_time_mg,
  coalesce(qa.qa_score_sum, 0) as qa_score_sum,
  coalesce(qa.qa_reviews_count, 0) as qa_reviews_count,
  coalesce(td.test_weighted_sum, 0) as test_weighted_sum,
  coalesce(td.test_questions_sum, 0) as test_questions_sum,
  coalesce(td.test_attempts_count, 0) as test_attempts_count
from employee_days_all as ed
any left join indiv_tasks_daily as it
  on ed.activity_date = it.activity_date
  and ed.employee_name_key = it.employee_name_key
any left join downtime_daily as idle
  on ed.activity_date = idle.activity_date
  and ed.employee_name_key = idle.employee_name_key
any left join closures_daily as cl
  on ed.activity_date = cl.activity_date
  and ed.employee_name_key = cl.employee_name_key
any left join valid_answers as va
  on ed.activity_date = va.activity_date
  and ed.employee_name_key = va.employee_name_key
any left join ticket_calls_daily as tc
  on ed.activity_date = tc.activity_date
  and ed.employee_name_key = tc.employee_name_key
any left join dialer_daily as sz
  on ed.activity_date = sz.activity_date
  and ed.employee_name_key = sz.employee_name_key
any left join pbx_daily as mg
  on ed.activity_date = mg.activity_date
  and ed.employee_name_key = mg.employee_name_key
any left join qa_daily as qa
  on ed.activity_date = qa.activity_date
  and ed.employee_name_key = qa.employee_name_key
any left join test_daily as td
  on ed.activity_date = td.activity_date
  and ed.employee_name_key = td.employee_name_key
