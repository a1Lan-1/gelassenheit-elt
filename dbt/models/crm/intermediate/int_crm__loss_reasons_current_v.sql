{{
  config(
    materialized='view',
    alias='cur_loss_reasons_v',
    tags=['cur_loss_reasons', 'current_view'],
  )
}}

select *
from {{ ref('int_crm__loss_reasons_current') }}
final
