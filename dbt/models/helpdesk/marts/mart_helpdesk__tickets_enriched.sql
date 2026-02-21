{{
  config(
    materialized='view',
    alias='mart_tickets_enriched',
    tags=['mart_fsd', 'helpdesk', 'tickets_enriched', 'tickets_enriched_core'],
  )
}}

{# BI facade; FINAL — one row per id before merge. #}
select *
from {{ ref('int_helpdesk__tickets_enriched_current') }}
final
