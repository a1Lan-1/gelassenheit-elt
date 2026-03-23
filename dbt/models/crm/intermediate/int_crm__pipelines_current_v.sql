{{
  config(
    materialized='view',
    alias='cur_pipelines_v',
    tags=['cur_pipelines', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__pipelines_current') }}
final
