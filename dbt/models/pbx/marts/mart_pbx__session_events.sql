{{
  config(
    materialized='table',
    alias='session_events',
    engine=ch_engine_merge_tree(),
    order_by='(entry_id, event_seq)',
    settings={'allow_nullable_key': 1},
    tags=['mart_pbx', 'pbx', 'session_events'],
  )
}}

-- Thin mart alias for Metabase (cur_call_events is already incremental and narrow).
select *
from {{ ref('int_pbx__call_events') }}
