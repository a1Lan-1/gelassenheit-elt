{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='task_id',
    alias='int_task_ci_primary',
    engine=ch_engine_merge_tree(),
    order_by='(task_id)',
    settings={'allow_nullable_key': 1},
    tags=['helpdesk', 'tasks_enriched', 'tasks_enriched_core', 'current'],
  )
}}

{# Recalculating primary CI by task_id with recent link changes/cashdesk. #}
{% set lookback_days = var('task_ci_primary_lookback_days', var('operational_load_lookback_days', 3)) | int %}

with
{% if is_incremental() %}
affected as (
  select distinct task_id
  from (
    select task_id
    from {{ ref('int_helpdesk__task_config_item_current') }}
    where deleted_at is null
      and updated_at >= today() - {{ lookback_days }}
    order by task_id, updated_at desc
    limit 1 by task_id
    union distinct
    select tci.task_id
    from (
      select task_id, config_item_id
      from {{ ref('int_helpdesk__task_config_item_current') }}
      where deleted_at is null
      order by task_id, config_item_id, updated_at desc
      limit 1 by task_id, config_item_id
    ) as tci
    inner join (
      select item_id, updated_at
      from {{ ref('int_helpdesk__config_items_cashdesk') }}
      where updated_at >= today() - {{ lookback_days }}
      order by id, updated_at desc
      limit 1 by id
    ) as ci on ci.item_id = tci.config_item_id
  )
  where task_id is not null
),
{% endif %}

task_ci as (
  select task_id, config_item_id
  from {{ ref('int_helpdesk__task_config_item_current') }}
  {% if is_incremental() %}
  prewhere task_id in (select task_id from affected)
  {% endif %}
  where deleted_at is null
  order by task_id, config_item_id, updated_at desc
  limit 1 by task_id, config_item_id
),

-- Only CI, required affected tasks — not all cashdesk layer
config_items as (
  select
    id,
    item_id,
    sku_id,
    serial_number,
    inventory_number,
    updated_at,
    cashdesk_id
  from {{ ref('int_helpdesk__config_items_cashdesk') }}
  prewhere item_id in (select config_item_id from task_ci)
  order by id, updated_at desc
  limit 1 by id
)

select
  tci.task_id as task_id,
  toUInt32(count()) as ci_count,
  argMax(ci.item_id, (ci.cashdesk_id > 0, ci.updated_at, ci.id)) as config_item_id,
  argMax(ci.serial_number, (ci.cashdesk_id > 0, ci.updated_at, ci.id)) as serial_number,
  argMax(ci.inventory_number, (ci.cashdesk_id > 0, ci.updated_at, ci.id)) as inventory_number,
  argMax(ci.sku_id, (ci.cashdesk_id > 0, ci.updated_at, ci.id)) as sku_id,
  argMax(ci.cashdesk_id, (ci.cashdesk_id > 0, ci.updated_at, ci.id)) as cashdesk_id
from task_ci as tci
inner join config_items as ci on ci.item_id = tci.config_item_id
group by tci.task_id
