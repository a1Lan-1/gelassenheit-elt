{{
  config(
    materialized='view',
    alias='cur_comments_v',
    tags=['helpdesk', 'current_view'],
  )
}}

select *
from {{ ref('int_helpdesk__comments_current') }}
final