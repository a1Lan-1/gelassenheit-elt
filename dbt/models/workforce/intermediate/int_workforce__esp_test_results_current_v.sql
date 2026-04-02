{{
  config(
    materialized='view',
    alias='esp_test_results_v',
    tags=['gs', 'current_view'],
  )
}}

select *
from {{ ref('int_workforce__esp_test_results_current') }}

