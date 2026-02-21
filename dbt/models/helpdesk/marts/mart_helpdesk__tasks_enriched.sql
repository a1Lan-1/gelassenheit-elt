{{
  config(
    materialized='view',
    alias='mart_tasks_enriched',
    tags=['mart_fsd', 'helpdesk', 'tasks_enriched', 'tasks_enriched_core'],
  )
}}

{# BI facade; FINAL — one row per id before merge. #}
select *
from {{ ref('int_helpdesk__tasks_enriched_current') }}
final
