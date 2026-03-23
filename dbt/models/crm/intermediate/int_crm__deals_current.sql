{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_deals',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['crm', 'deals', 'current'],
  )
}}

{# Empty tags from CRM list API must not wipe previously loaded tags.
   If bronze has [] / {"items":[]}, incremental keeps the latest non-empty tags. #}

with raw_source as (
  select `id`, `name`, `price`, `responsible_user_id`, `group_id`, `status_id`, `pipeline_id`, `loss_reason_id`, `source_id`, `created_by`, `updated_by`, `closed_at`, `created_at`, `updated_at`, `closest_task_at`, `is_deleted`, `custom_fields_values`, `score`, `account_id`, `labor_cost`, `is_price_modified_by_robot`, `tags`, `contacts`, `company_id`, `msb_deal_completed`, `msb_stage_date`, `msb_contact_date`, `msb_first_contact_date`, `msb_last_call_date`, `msb_contact_form`, `kkt_count`
  from {{ bronze_parquet('crm', 'deals', '`id` Nullable(UInt64), `name` Nullable(String), `price` Nullable(Int64), `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `status_id` Nullable(UInt64), `pipeline_id` Nullable(UInt64), `loss_reason_id` Nullable(UInt64), `source_id` Nullable(UInt64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `closed_at` String, `created_at` String, `updated_at` String, `closest_task_at` String, `is_deleted` Nullable(String), `custom_fields_values` Nullable(String), `score` Nullable(String), `account_id` Nullable(UInt64), `labor_cost` Nullable(String), `is_price_modified_by_robot` Nullable(String), `tags` Nullable(String), `contacts` Nullable(String), `company_id` Nullable(UInt64), `msb_deal_completed` String, `msb_stage_date` String, `msb_contact_date` String, `msb_first_contact_date` String, `msb_last_call_date` String, `msb_contact_form` Nullable(String), `kkt_count` Nullable(String)') }}
),
parsed as (
  select
    assumeNotNull(CAST(`id`, 'Nullable(UInt64)')) AS `id`,
    CAST(`name`, 'Nullable(String)') AS `name`,
    CAST(`price`, 'Nullable(Int64)') AS `price`,
    CAST(`responsible_user_id`, 'Nullable(UInt64)') AS `responsible_user_id`,
    CAST(`group_id`, 'Nullable(UInt64)') AS `group_id`,
    CAST(`status_id`, 'Nullable(UInt64)') AS `status_id`,
    CAST(`pipeline_id`, 'Nullable(UInt64)') AS `pipeline_id`,
    CAST(`loss_reason_id`, 'Nullable(UInt64)') AS `loss_reason_id`,
    CAST(`source_id`, 'Nullable(UInt64)') AS `source_id`,
    CAST(`created_by`, 'Nullable(UInt64)') AS `created_by`,
    CAST(`updated_by`, 'Nullable(UInt64)') AS `updated_by`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closed_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closed_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `closed_at`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3)), toDateTime64('1970-01-01 00:00:00', 3), parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3) + toIntervalHour(3)), 'DateTime64(3)') AS `updated_at`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closest_task_at`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`closest_task_at`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `closest_task_at`,
    CAST(multiIf(lower(trimBoth(toString(`is_deleted`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`is_deleted`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`is_deleted`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`is_deleted`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `is_deleted`,
    CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
    CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`score`)), ''), ',', '.')), 'Nullable(Float64)') AS `score`,
    CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
    CAST(toFloat64OrNull(replaceAll(nullIf(trimBoth(toString(`labor_cost`)), ''), ',', '.')), 'Nullable(Float64)') AS `labor_cost`,
    CAST(multiIf(lower(trimBoth(toString(`is_price_modified_by_robot`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`is_price_modified_by_robot`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`is_price_modified_by_robot`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`is_price_modified_by_robot`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)') AS `is_price_modified_by_robot`,
    CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`tags`)), '')") }}, 'Nullable(JSON)') AS `tags`,
    CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`contacts`)), '')") }}, 'Nullable(JSON)') AS `contacts`,
    CAST(`company_id`, 'Nullable(UInt64)') AS `company_id`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_deal_completed`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_deal_completed`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_deal_completed`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_stage_date`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_stage_date`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_stage_date`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_contact_date`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_contact_date`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_contact_date`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_first_contact_date`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_first_contact_date`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_first_contact_date`,
    CAST(if(isNull(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_last_call_date`)), ''), 3)), NULL, parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`msb_last_call_date`)), ''), 3) + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_last_call_date`,
    CAST(`msb_contact_form`, 'Nullable(String)') AS `msb_contact_form`,
    CAST(`kkt_count`, 'Nullable(String)') AS `kkt_count`
  from raw_source
)
{% if is_incremental() %}
,
prev_tags as (
  select
    id,
    argMax(tags, (length(toString(tags)) > 15, updated_at)) as tags
  from {{ this }}
  group by id
)
{% endif %}
select
  p.id,
  p.name,
  p.price,
  p.responsible_user_id,
  p.group_id,
  p.status_id,
  p.pipeline_id,
  p.loss_reason_id,
  p.source_id,
  p.created_by,
  p.updated_by,
  p.closed_at,
  p.created_at,
  p.updated_at,
  p.closest_task_at,
  p.is_deleted,
  p.custom_fields_values,
  p.score,
  p.account_id,
  p.labor_cost,
  p.is_price_modified_by_robot,
  {% if is_incremental() %}
  if(
    length(toString(p.tags)) > 15,
    p.tags,
    coalesce(pt.tags, p.tags)
  ) as tags,
  {% else %}
  p.tags as tags,
  {% endif %}
  p.contacts,
  p.company_id,
  p.msb_deal_completed,
  p.msb_stage_date,
  p.msb_contact_date,
  p.msb_first_contact_date,
  p.msb_last_call_date,
  p.msb_contact_form,
  p.kkt_count
from parsed as p
{% if is_incremental() %}
left join prev_tags as pt on pt.id = p.id
{% endif %}
