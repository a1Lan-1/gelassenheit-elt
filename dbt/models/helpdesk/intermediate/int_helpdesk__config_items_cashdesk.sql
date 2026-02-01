{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='int_config_items_cashdesk',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'tasks_enriched', 'tasks_enriched_core', 'current'],
  )
}}

{# cashdeskId is extracted once; watermark append is like tickets_enriched.
   Without FULL FINAL: filter by watermark, then limit 1 by id. #}
select
  id,
  item_id,
  sku_id,
  serial_number,
  inventory_number,
  updated_at,
  assumeNotNull(
    toUInt64OrZero(ifNull(JSONExtractString(toString(data), 'cashdeskId'), ''))
  ) as cashdesk_id
from (
  select *
  from {{ ref('int_helpdesk__config_items_current') }}
  where deleted_at is null
    {% if is_incremental() %}
    and updated_at > (
      select coalesce(max(updated_at), toDateTime64('1970-01-01 00:00:00', 3))
      from {{ this }}
    )
    {% endif %}
  order by id, updated_at desc
  limit 1 by id
)
