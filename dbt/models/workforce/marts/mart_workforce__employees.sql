{{
  config(
    materialized='table',
    alias='mart_employees',
    engine=ch_engine_merge_tree(),
    order_by='(organization, employee_key)',
    tags=['mart_gs', 'gs', 'employees'],
  )
}}

with esp as (
  select
    'esp' as organization,
    full_name as employee_key,
    full_name,
    cast(null as Nullable(String)) as email,
    line as group_name,
    supervisor,
    employment_status,
    cast(null as Nullable(Date)) as training_start_date,
    cast(null as Nullable(Date)) as training_end_date,
    hired_at as line_start_date,
    birthday,
    cast(null as Nullable(Date)) as dismissal_date,
    cast(null as Nullable(String)) as telegram_login,
    cast(null as Nullable(String)) as mm_login,
    cast(null as Nullable(String)) as phone,
    lower(trimBoth(coalesce(employment_status, ''))) in ('dismissed', 'terminated', 'fired') as is_dismissed,
    ingested_at
  from {{ ref('int_workforce__esp_techsup_employees_current_v') }}
  where full_name is not null and trimBoth(full_name) != ''
),
akc as (
  select
    'akc' as organization,
    assumeNotNull(coalesce(
      nullIf(trimBoth(email), ''),
      nullIf(trimBoth(full_name), ''),
      concat('akc:', toString(cityHash64(supervisor, full_name, group_name, line_start_date)))
    )) as employee_key,
    full_name,
    email,
    group_name,
    supervisor,
    employment_status,
    training_start_date,
    training_end_date,
    line_start_date,
    birthday,
    cast(null as Nullable(Date)) as dismissal_date,
    telegram_login,
    mm_login,
    phone,
    lower(trimBoth(coalesce(employment_status, ''))) in (
      'dismissed', 'terminated', 'fired'
    ) as is_dismissed,
    ingested_at
  from {{ ref('int_workforce__akc_employees_current_v') }}
)
select * from esp
union all
select * from akc
