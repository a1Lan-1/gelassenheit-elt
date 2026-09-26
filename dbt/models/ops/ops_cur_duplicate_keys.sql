{{
  config(
    materialized='table',
    alias='ops_cur_duplicate_keys',
    engine=ch_engine_merge_tree(),
    order_by='(entity, id)',
    tags=['ops', 'duplicate_keys'],
  )
}}

select
  'appeals' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__appeals_current') }}
group by id
having count() > 1
union all
select
  'comments' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__comments_current') }}
group by id
having count() > 1
union all
select
  'config_items' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__config_items_current') }}
group by id
having count() > 1
union all
select
  'entities' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__entities_current') }}
group by id
having count() > 1
union all
select
  'history' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__history_current') }}
group by id
having count() > 1
union all
select
  'persons' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__persons_current') }}
group by id
having count() > 1
union all
select
  'places' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__places_current') }}
group by id
having count() > 1
union all
select
  'services' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__services_current') }}
group by id
having count() > 1
union all
select
  'sku' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__sku_current') }}
group by id
having count() > 1
union all
select
  'statuses' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__statuses_current') }}
group by id
having count() > 1
union all
select
  'task_config_item' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__task_config_item_current') }}
group by id
having count() > 1
union all
select
  'task_status' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__task_status_current') }}
group by id
having count() > 1
union all
select
  'tasks' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__tasks_current') }}
group by id
having count() > 1
union all
select
  'ticket_cases' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__ticket_cases_current') }}
group by id
having count() > 1
union all
select
  'ticket_categories' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__ticket_categories_current') }}
group by id
having count() > 1
union all
select
  'ticket_channels' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__ticket_channels_current') }}
group by id
having count() > 1
union all
select
  'ticket_config_item' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__ticket_config_item_current') }}
group by id
having count() > 1
union all
select
  'ticket_status' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__ticket_status_current') }}
group by id
having count() > 1
union all
select
  'tickets' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__tickets_current') }}
group by id
having count() > 1
union all
select
  'types' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__types_current') }}
group by id
having count() > 1
union all
select
  'user_groups' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__user_groups_current') }}
group by id
having count() > 1
union all
select
  'user_groups_mapping' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__user_groups_mapping_current') }}
group by id
having count() > 1
union all
select
  'user_types' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__user_types_current') }}
group by id
having count() > 1
union all
select
  'users' as entity,
  id,
  count() as duplicate_count
from {{ ref('int_helpdesk__users_current') }}
group by id
having count() > 1
