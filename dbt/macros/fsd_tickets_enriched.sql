{% macro fsd_dim_label(expr, default) -%}
coalesce(nullIf(trimBoth({{ expr }}), ''), '{{ default }}')
{%- endmacro %}

{% macro fsd_tickets_enriched_base() %}
{# Id-first: no FINAL on full tickets_current; dedup by batch id only. #}
with
{% if is_incremental() %}
touched_ids as (
  select distinct id
  from {{ ref('int_helpdesk__tickets_current') }}
  where deleted_at is null
    and updated_at > (
      select coalesce(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
      from {{ this }}
    )
),
{% endif %}

tickets as (
  select
    id,
    parent_id,
    entity_id,
    description,
    ticket_category_id,
    person_id,
    place_id,
    data,
    created_at,
    updated_at,
    deleted_at,
    service_id,
    contract_id,
    time_zone,
    status_id,
    graph_group_id,
    `case`,
    title,
    priority,
    impact,
    urgency,
    number,
    user_group_id,
    responsible_user_id,
    closure_code,
    deadline,
    ext_number,
    actual_status_id,
    last_comment_id,
    closed_status_id,
    client_bill_sum,
    supplier_bill_sum,
    last_todo_id,
    resolved_status_id,
    ticket_case_id,
    user_group_cycle_count,
    channel_id
  from (
    select
      id,
      parent_id,
      entity_id,
      description,
      ticket_category_id,
      person_id,
      place_id,
      data,
      created_at,
      updated_at,
      deleted_at,
      service_id,
      contract_id,
      time_zone,
      status_id,
      graph_group_id,
      `case`,
      title,
      priority,
      impact,
      urgency,
      number,
      user_group_id,
      responsible_user_id,
      closure_code,
      deadline,
      ext_number,
      actual_status_id,
      last_comment_id,
      closed_status_id,
      client_bill_sum,
      supplier_bill_sum,
      last_todo_id,
      resolved_status_id,
      ticket_case_id,
      user_group_cycle_count,
      channel_id
    from {{ ref('int_helpdesk__tickets_current') }}
    {% if is_incremental() %}
    prewhere id in (select id from touched_ids)
    {% else %}
    final
    {% endif %}
    where deleted_at is null
    {% if is_incremental() %}
    order by id, updated_at desc
    limit 1 by id
    {% endif %}
  )
),

statuses as (
  select id, name
  from {{ ref('int_helpdesk__statuses_current_v') }}
),

categories as (
  select id, name
  from {{ ref('int_helpdesk__ticket_categories_current_v') }}
),

services as (
  select
    id,
    name,
    coalesce(nullIf(trimBoth(name), ''), '') as name_norm
  from {{ ref('int_helpdesk__services_current_v') }}
),

cases as (
  select id, name
  from {{ ref('int_helpdesk__ticket_cases_current_v') }}
),

users as (
  select id, last_name, first_name, middle_name
  from {{ ref('int_helpdesk__users_current_v') }}
),

user_groups as (
  select id, name
  from {{ ref('int_helpdesk__user_groups_current_v') }}
),

user_groups_mapping as (
  select user_id, user_group_id, updated_at, deleted_at
  from {{ ref('int_helpdesk__user_groups_mapping_current_v') }}
),

resp_group as (
  select
    user_id,
    argMax(user_group_id, updated_at) as user_group_id
  from user_groups_mapping
  where deleted_at is null
  group by user_id
),

entities as (
  select id, name
  from {{ ref('int_helpdesk__entities_current_v') }}
),

places as (
  select id, city, address
  from {{ ref('int_helpdesk__places_current_v') }}
),

persons as (
  select id, last_name, first_name, middle_name, position
  from {{ ref('int_helpdesk__persons_current_v') }}
),

channels as (
  select id, name
  from {{ ref('int_helpdesk__ticket_channels_current_v') }}
)

select
  t.id as id,
  t.parent_id as parent_id,
  t.entity_id as entity_id,
  t.description as description,
  t.ticket_category_id as ticket_category_id,
  t.person_id as person_id,
  t.place_id as place_id,
  t.data as data,
  t.created_at as created_at,
  t.updated_at as updated_at,
  t.deleted_at as deleted_at,
  t.service_id as service_id,
  t.contract_id as contract_id,
  t.time_zone as time_zone,
  t.status_id as status_id,
  t.graph_group_id as graph_group_id,
  t.`case` as `case`,
  t.title as title,
  t.priority as priority,
  t.impact as impact,
  t.urgency as urgency,
  t.number as number,
  t.user_group_id as user_group_id,
  t.responsible_user_id as responsible_user_id,
  t.closure_code as closure_code,
  t.deadline as deadline,
  t.ext_number as ext_number,
  t.actual_status_id as actual_status_id,
  t.last_comment_id as last_comment_id,
  t.closed_status_id as closed_status_id,
  t.client_bill_sum as client_bill_sum,
  t.supplier_bill_sum as supplier_bill_sum,
  t.last_todo_id as last_todo_id,
  t.resolved_status_id as resolved_status_id,
  t.ticket_case_id as ticket_case_id,
  t.user_group_cycle_count as user_group_cycle_count,
  t.channel_id as channel_id,
  {{ fsd_dim_label('st.name', 'no status') }} as current_status_name,
  {{ fsd_dim_label('tc.name', 'no category') }} as category_name,
  {{ fsd_dim_label('srv.name', 'no service') }} as service_name,
  coalesce(
    nullIf(
      trimBoth(
        multiIf(
          position(srv.name_norm, '/') > 0,
          splitByChar('/', srv.name_norm)[1],
          srv.name_norm
        )
      ),
      ''
    ),
    'no service'
  ) as service_theme,
  {{ fsd_dim_label('tcase.name', 'no case') }} as case_name,
  coalesce(
    nullIf(trim(concat(resp_user.last_name, ' ', resp_user.first_name, ' ', resp_user.middle_name)), ''),
    'no responsible'
  ) as responsible_user_full_name,
  {{ fsd_dim_label('ug.name', 'no group') }} as user_group_name,
  {{ fsd_dim_label('resp_ug.name', 'no responsible group') }} as responsible_group_name,
  {{ fsd_dim_label('e.name', 'no entity') }} as entity_name,
  coalesce(p.city, '') as place_city,
  coalesce(p.address, '') as place_address,
  coalesce(
    nullIf(trim(concat(pers.last_name, ' ', pers.first_name, ' ', pers.middle_name, ' ', pers.position)), ''),
    'no person'
  ) as person_full_name,
  {{ fsd_dim_label('ch.name', 'no channel') }} as new_channel
from tickets as t
any left join statuses as st on t.status_id = st.id
any left join categories as tc on t.ticket_category_id = tc.id
any left join services as srv on t.service_id = srv.id
any left join cases as tcase on t.ticket_case_id = tcase.id
any left join users as resp_user on t.responsible_user_id = resp_user.id
any left join user_groups as ug on t.user_group_id = ug.id
any left join resp_group as rg on rg.user_id = t.responsible_user_id
any left join user_groups as resp_ug on resp_ug.id = rg.user_group_id
any left join entities as e on t.entity_id = e.id
any left join places as p on t.place_id = p.id
any left join persons as pers on t.person_id = pers.id
any left join channels as ch on t.channel_id = ch.id
{% endmacro %}

{% macro fsd_backlog_snapshot_hour() -%}
{%- set raw = var('backlog_snapshot_hour', '') -%}
{%- if raw -%}
parseDateTime64BestEffort('{{ raw }}', 3, 'Europe/Moscow')
{%- else -%}
toStartOfHour(now('Europe/Moscow'))
{%- endif -%}
{%- endmacro %}
