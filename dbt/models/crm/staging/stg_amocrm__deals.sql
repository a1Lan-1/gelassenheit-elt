{{
  config(
    materialized='table',
    alias='stg_deals',
    engine=ch_engine_merge_tree(),
    order_by='(id)',
    settings={'allow_nullable_key': 1},
    tags=['crm', 'deals', 'staging'],
  )
}}

with raw_source as (
  select *
  from {{ s3_parquet('crm', 'deals', '`id` Nullable(UInt64), `name` Nullable(String), `price` Nullable(Int64), `responsible_user_id` Nullable(UInt64), `group_id` Nullable(UInt64), `status_id` Nullable(UInt64), `pipeline_id` Nullable(UInt64), `loss_reason_id` Nullable(UInt64), `source_id` Nullable(UInt64), `created_by` Nullable(UInt64), `updated_by` Nullable(UInt64), `closed_at` Nullable(DateTime64(3)), `created_at` Nullable(DateTime64(3)), `updated_at` Nullable(DateTime64(3)), `closest_task_at` Nullable(DateTime64(3)), `is_deleted` Nullable(Bool), `custom_fields_values` Nullable(String), `score` Nullable(Float64), `account_id` Nullable(UInt64), `labor_cost` Nullable(Float64), `is_price_modified_by_robot` Nullable(Bool), `tags` Nullable(String), `contacts` Nullable(String), `company_id` Nullable(UInt64), `msb_deal_completed` Nullable(DateTime64(3)), `msb_stage_date` Nullable(DateTime64(3)), `msb_contact_date` Nullable(DateTime64(3)), `msb_first_contact_date` Nullable(DateTime64(3)), `msb_last_call_date` Nullable(DateTime64(3)), `msb_contact_form` Nullable(String), `kkt_count` Nullable(String)') }}
)
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
  CAST(if(isNull(`closed_at`), NULL, `closed_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `closed_at`,
  CAST(if(isNull(`created_at`), NULL, `created_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `created_at`,
  CAST(if(isNull(`updated_at`), NULL, `updated_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `updated_at`,
  CAST(if(isNull(`closest_task_at`), NULL, `closest_task_at` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `closest_task_at`,
  CAST(`is_deleted`, 'Nullable(Bool)') AS `is_deleted`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`custom_fields_values`)), '')") }}, 'Nullable(JSON)') AS `custom_fields_values`,
  CAST(`score`, 'Nullable(Float64)') AS `score`,
  CAST(`account_id`, 'Nullable(UInt64)') AS `account_id`,
  CAST(`labor_cost`, 'Nullable(Float64)') AS `labor_cost`,
  CAST(`is_price_modified_by_robot`, 'Nullable(Bool)') AS `is_price_modified_by_robot`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`tags`)), '')") }}, 'Nullable(JSON)') AS `tags`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`contacts`)), '')") }}, 'Nullable(JSON)') AS `contacts`,
  CAST(`company_id`, 'Nullable(UInt64)') AS `company_id`,
  CAST(if(isNull(`msb_deal_completed`), NULL, `msb_deal_completed` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_deal_completed`,
  CAST(if(isNull(`msb_stage_date`), NULL, `msb_stage_date` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_stage_date`,
  CAST(if(isNull(`msb_contact_date`), NULL, `msb_contact_date` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_contact_date`,
  CAST(if(isNull(`msb_first_contact_date`), NULL, `msb_first_contact_date` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_first_contact_date`,
  CAST(if(isNull(`msb_last_call_date`), NULL, `msb_last_call_date` + toIntervalHour(3)), 'Nullable(DateTime64(3))') AS `msb_last_call_date`,
  CAST(`msb_contact_form`, 'Nullable(String)') AS `msb_contact_form`,
  CAST(`kkt_count`, 'Nullable(String)') AS `kkt_count`
from raw_source
