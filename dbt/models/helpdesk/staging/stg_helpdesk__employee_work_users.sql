{{
  config(
    materialized='view',
    alias='stg_employee_work_users',
    tags=['helpdesk', 'work_blocks', 'staging'],
  )
}}

with src as (
  select
    id,
    user_name as login_name_raw,
    first_name,
    middle_name,
    last_name,
    email,
    deleted_at
  from {{ ref('int_helpdesk__users_current') }}
  final
)

select
  src.id as user_id,
  trim(concat(src.last_name, ' ', src.first_name, ' ', src.middle_name)) as user_name,
  nullIf(trimBoth(src.login_name_raw), '') as login_name,
  lowerUTF8(trimBoth(trim(concat(src.last_name, ' ', src.first_name, ' ', src.middle_name)))) as user_name_key,
  lowerUTF8(trimBoth(coalesce(nullIf(trimBoth(src.login_name_raw), ''), ''))) as login_name_key,
  src.email as email
from src
where src.deleted_at is null
  and trim(concat(src.last_name, ' ', src.first_name, ' ', src.middle_name)) != ''
