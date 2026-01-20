{% macro fsd_lc_label(expr, default) -%}
CAST({{ fsd_dim_label(expr, default) }} AS LowCardinality(String))
{%- endmacro %}

{% macro fsd_tasks_enriched_base() %}
{# Id-first + prune dims to batch keys only. #}
with
{% if is_incremental() %}
touched_ids as (
  select distinct id
  from {{ ref('int_helpdesk__tasks_current') }}
  where deleted_at is null
    and updated_at > (
      select coalesce(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
      from {{ this }}
    )
),
{% endif %}

tasks as (
  select
    id,
    number,
    name,
    ticket_id,
    created_at,
    updated_at,
    deadline,
    plan_start_date,
    plan_end_date,
    status_id,
    priority,
    mass_type,
    user_group_id,
    responsible_user_id,
    person_id,
    place_id,
    partner_code,
    partner_delivery,
    vendor
  from (
    select
      id,
      number,
      name,
      ticket_id,
      created_at,
      updated_at,
      deadline,
      plan_start_date,
      plan_end_date,
      status_id,
      priority,
      mass_type,
      user_group_id,
      responsible_user_id,
      person_id,
      place_id,
      partner_code,
      partner_delivery,
      vendor,
      deleted_at
    from {{ ref('int_helpdesk__tasks_current') }}
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

places as (
  select id, city, address
  from {{ ref('int_helpdesk__places_current_v') }}
),

persons as (
  select id, last_name, first_name, middle_name, position
  from {{ ref('int_helpdesk__persons_current_v') }}
),

tickets as (
  select
    id,
    number,
    entity_id,
    service_id,
    ticket_category_id,
    status_id
  from (
    select
      id,
      number,
      entity_id,
      service_id,
      ticket_category_id,
      status_id,
      updated_at
    from {{ ref('int_helpdesk__tickets_current') }}
    {% if is_incremental() %}
    prewhere id in (select ticket_id from tasks where ticket_id is not null)
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

entities as (
  select id, name
  from {{ ref('int_helpdesk__entities_current_v') }}
),

task_ci_primary as (
  select *
  from {{ ref('int_helpdesk__task_ci_primary') }}
  {% if is_incremental() %}
  where task_id in (select id from tasks)
  {% endif %}
),

sku as (
  select id, name, model, type, manufacturer
  from {{ ref('int_helpdesk__sku_current_v') }}
),

cashdesk_by_znid as (
  select
    assumeNotNull(id::UInt64) as cashdesk_id,
    nullIf(trimBoth(znid), '') as znid
  from mv_cashdesk.cashdesk final
  where id > 0
    and nullIf(trimBoth(znid), '') is not null
    {% if is_incremental() %}
    and nullIf(trimBoth(znid), '') in (
      select nullIf(trimBoth(serial_number), '')
      from task_ci_primary
      where nullIf(trimBoth(serial_number), '') is not null
    )
    {% endif %}
),

kkt as (
  select
    cashdesk_id,
    pmsr_name,
    esm_version,
    tsp_model
  from {{ ref('mart_devices__kkt_info') }}
  {% if is_incremental() %}
  where cashdesk_id in (
    select cashdesk_id from task_ci_primary where cashdesk_id > 0
  )
  {% endif %}
),

partners as (
  select partner_code, inn
  from mv_portal.partners final
  {% if is_incremental() %}
  where partner_code in (
    select nullIf(trimBoth(partner_code), '')
    from tasks
    where nullIf(trimBoth(partner_code), '') is not null
  )
  {% endif %}
),

companies as (
  select inn, anyHeavy(name) as name
  from mv_portal.company final
  where inn in (select inn from partners)
  group by inn
),

task_base as (
  select
    t.id as id,
    t.number as number,
    coalesce(t.name, '') as name,
    t.ticket_id as ticket_id,
    coalesce(toString(ti.number), '') as ticket_number,
    t.created_at as created_at,
    t.updated_at as updated_at,
    t.deadline as deadline,
    t.plan_start_date as plan_start_date,
    t.plan_end_date as plan_end_date,
    {{ fsd_lc_label('st.name', 'no status') }} as status_name,
    t.priority as priority,
    {{ fsd_lc_label('t.mass_type', 'no mass type') }} as mass_type,
    {{ fsd_lc_label('ug.name', 'no group') }} as user_group_name,
    coalesce(
      nullIf(trim(concat(resp_user.last_name, ' ', resp_user.first_name, ' ', resp_user.middle_name)), ''),
      'no responsible'
    ) as responsible_user_full_name,
    {{ fsd_lc_label('resp_ug.name', 'no responsible group') }} as responsible_group_name,
    coalesce(
      nullIf(trim(concat(pers.last_name, ' ', pers.first_name, ' ', pers.middle_name, ' ', pers.position)), ''),
      'no person'
    ) as person_full_name,
    coalesce(p.city, '') as place_city,
    coalesce(p.address, '') as place_address,
    ti.entity_id as entity_id,
    {{ fsd_lc_label('e.name', 'no entity') }} as entity_name,
    {{ fsd_lc_label('srv.name', 'no service') }} as service_name,
    CAST(
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
      ) AS LowCardinality(String)
    ) as service_theme,
    {{ fsd_lc_label('tc.name', 'no category') }} as category_name,
    {{ fsd_lc_label('tst.name', 'no status') }} as ticket_status_name,
    {{ fsd_lc_label('t.partner_code', 'no partner') }} as partner_code,
    {{ fsd_lc_label('co.name', 'no partner') }} as partner_name,
    t.partner_delivery as partner_delivery,
    pca.config_item_id as config_item_id,
    {{ fsd_lc_label('pca.serial_number', '') }} as serial_number,
    {{ fsd_lc_label('pca.inventory_number', '') }} as inventory_number,
    {{ fsd_lc_label('sku.name', 'no sku') }} as sku_name,
    {{ fsd_lc_label('sku.model', 'no model') }} as sku_model,
    {{ fsd_lc_label('sku.type', 'no type') }} as sku_type,
    if(pca.cashdesk_id > 0, pca.cashdesk_id, coalesce(cd.cashdesk_id, toUInt64(0))) as cashdesk_id,
    coalesce(pca.ci_count, toUInt32(0)) as ci_count,
    CAST(
      coalesce(
        nullIf(trimBoth(t.vendor), ''),
        nullIf(trimBoth(sku.manufacturer), ''),
        'no vendor'
      ) AS LowCardinality(String)
    ) as vendor
  from tasks as t
  any left join statuses as st on t.status_id = st.id
  any left join users as resp_user on t.responsible_user_id = resp_user.id
  any left join user_groups as ug on t.user_group_id = ug.id
  any left join resp_group as rg on rg.user_id = t.responsible_user_id
  any left join user_groups as resp_ug on resp_ug.id = rg.user_group_id
  any left join places as p on t.place_id = p.id
  any left join persons as pers on t.person_id = pers.id
  any left join tickets as ti on t.ticket_id = ti.id
  any left join statuses as tst on ti.status_id = tst.id
  any left join categories as tc on ti.ticket_category_id = tc.id
  any left join services as srv on ti.service_id = srv.id
  any left join entities as e on ti.entity_id = e.id
  any left join task_ci_primary as pca on pca.task_id = t.id
  any left join sku as sku on sku.id = pca.sku_id
  any left join cashdesk_by_znid as cd on cd.znid = nullIf(trimBoth(pca.serial_number), '')
  any left join partners as pr on pr.partner_code = nullIf(trimBoth(t.partner_code), '')
  any left join companies as co on co.inn = pr.inn
)

select
  tb.id as id,
  tb.number as number,
  tb.name as name,
  tb.ticket_id as ticket_id,
  tb.ticket_number as ticket_number,
  tb.created_at as created_at,
  tb.updated_at as updated_at,
  tb.deadline as deadline,
  tb.plan_start_date as plan_start_date,
  tb.plan_end_date as plan_end_date,
  tb.status_name as status_name,
  tb.priority as priority,
  tb.mass_type as mass_type,
  tb.user_group_name as user_group_name,
  tb.responsible_user_full_name as responsible_user_full_name,
  tb.responsible_group_name as responsible_group_name,
  tb.person_full_name as person_full_name,
  tb.place_city as place_city,
  tb.place_address as place_address,
  tb.entity_id as entity_id,
  tb.entity_name as entity_name,
  tb.service_name as service_name,
  tb.service_theme as service_theme,
  tb.category_name as category_name,
  tb.ticket_status_name as ticket_status_name,
  tb.partner_code as partner_code,
  tb.partner_name as partner_name,
  tb.partner_delivery as partner_delivery,
  tb.config_item_id as config_item_id,
  tb.serial_number as serial_number,
  tb.inventory_number as inventory_number,
  tb.sku_name as sku_name,
  tb.sku_model as sku_model,
  tb.sku_type as sku_type,
  tb.cashdesk_id as cashdesk_id,
  tb.ci_count as ci_count,
  tb.vendor as vendor,
  {{ fsd_lc_label('k.pmsr_name', '') }} as pmsr_name,
  {{ fsd_lc_label('k.esm_version', '') }} as esm_version,
  {{ fsd_lc_label('k.tsp_model', '') }} as tsp_model
from task_base as tb
any left join kkt as k on tb.cashdesk_id > 0 and k.cashdesk_id = tb.cashdesk_id
{% endmacro %}
