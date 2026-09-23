{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_tasks',
    engine=ch_engine_replacing('updated_at'),
    order_by='(id)',
    unique_key=['id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'tasks', 'current'],
  )
}}

with raw_source as (
  select `id`, `case_id`, `description`, `user_group_id`, `responsible_user_id`, `person_id`, `place_id`, `ticket_id`, `data`, `deadline`, `plan_start_date`, `plan_end_date`, `created_at`, `updated_at`, `deleted_at`, `status_id`, `number`, `name`, `closure_code`, `ext_number`, `actual_status_id`, `resolved_status_id`, `partner_delivery`, `last_comment_id`, `closed_status_id`, `client_bill_sum`, `supplier_bill_sum`, `last_todo_id`, `partner_pickup_date`, `mass_type`, `mass_head_id`, `priority`, `partner_code`, `vendor`
  from {{ bronze_parquet('helpdesk', 'tasks', '`id` String, `case_id` String, `description` Nullable(String), `user_group_id` String, `responsible_user_id` String, `person_id` String, `place_id` String, `ticket_id` String, `data` Nullable(String), `deadline` String, `plan_start_date` String, `plan_end_date` String, `created_at` String, `updated_at` String, `deleted_at` String, `status_id` String, `number` Nullable(Int32), `name` Nullable(String), `closure_code` String, `ext_number` Nullable(String), `actual_status_id` String, `resolved_status_id` String, `partner_delivery` Nullable(String), `last_comment_id` String, `closed_status_id` String, `client_bill_sum` Nullable(Decimal(12,2)), `supplier_bill_sum` Nullable(Decimal(12,2)), `last_todo_id` String, `partner_pickup_date` String, `mass_type` Nullable(String), `mass_head_id` String, `priority` Nullable(Int32), `partner_code` Nullable(String), `vendor` Nullable(String)') }}
)
select
  assumeNotNull(CAST(toUUIDOrNull(nullIf(trimBoth(toString(`id`)), '')), 'Nullable(UUID)')) AS `id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`case_id`)), '')), 'Nullable(UUID)') AS `case_id`,
  CAST(`description`, 'Nullable(String)') AS `description`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`user_group_id`)), '')), 'Nullable(UUID)') AS `user_group_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`responsible_user_id`)), '')), 'Nullable(UUID)') AS `responsible_user_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`person_id`)), '')), 'Nullable(UUID)') AS `person_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`place_id`)), '')), 'Nullable(UUID)') AS `place_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`ticket_id`)), '')), 'Nullable(UUID)') AS `ticket_id`,
  CAST({{ ch_json_from_string("nullIf(trimBoth(toString(`data`)), '')") }}, 'Nullable(JSON)') AS `data`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`deadline`)), ''), 3), 'Nullable(DateTime64(3))') AS `deadline`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`plan_start_date`)), ''), 3), 'Nullable(DateTime64(3))') AS `plan_start_date`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`plan_end_date`)), ''), 3), 'Nullable(DateTime64(3))') AS `plan_end_date`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`created_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `created_at`,
  CAST(coalesce(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`updated_at`)), ''), 3), toDateTime64('1970-01-01 00:00:00', 3)), 'DateTime64(3)') AS `updated_at`,
  CAST({{ ch_parse_deleted_at_or_null('`deleted_at`') }}, 'Nullable(DateTime64(3))') AS `deleted_at`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`status_id`)), '')), 'Nullable(UUID)') AS `status_id`,
  assumeNotNull(CAST(`number`, 'Nullable(Int32)')) AS `number`,
  CAST(`name`, 'Nullable(String)') AS `name`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`closure_code`)), '')), 'Nullable(UUID)') AS `closure_code`,
  CAST(`ext_number`, 'Nullable(String)') AS `ext_number`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`actual_status_id`)), '')), 'Nullable(UUID)') AS `actual_status_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`resolved_status_id`)), '')), 'Nullable(UUID)') AS `resolved_status_id`,
  assumeNotNull(CAST(multiIf(lower(trimBoth(toString(`partner_delivery`))) IN ('true', '1', 'yes', 'y', 't'), true, lower(trimBoth(toString(`partner_delivery`))) IN ('false', '0', 'no', 'n', 'f'), false, lower(trimBoth(toString(`partner_delivery`))) IN ('dismissed', 'terminated', 'fired'), true, lower(trimBoth(toString(`partner_delivery`))) IN ('active', 'working', 'employed'), false, null), 'Nullable(Bool)')) AS `partner_delivery`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`last_comment_id`)), '')), 'Nullable(UUID)') AS `last_comment_id`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`closed_status_id`)), '')), 'Nullable(UUID)') AS `closed_status_id`,
  CAST(`client_bill_sum`, 'Nullable(Decimal(12,2))') AS `client_bill_sum`,
  CAST(`supplier_bill_sum`, 'Nullable(Decimal(12,2))') AS `supplier_bill_sum`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`last_todo_id`)), '')), 'Nullable(UUID)') AS `last_todo_id`,
  CAST(parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(`partner_pickup_date`)), ''), 3), 'Nullable(DateTime64(3))') AS `partner_pickup_date`,
  CAST(`mass_type`, 'Nullable(String)') AS `mass_type`,
  CAST(toUUIDOrNull(nullIf(trimBoth(toString(`mass_head_id`)), '')), 'Nullable(UUID)') AS `mass_head_id`,
  CAST(`priority`, 'Nullable(Int32)') AS `priority`,
  CAST(`partner_code`, 'Nullable(String)') AS `partner_code`,
  CAST(`vendor`, 'Nullable(String)') AS `vendor`
from raw_source