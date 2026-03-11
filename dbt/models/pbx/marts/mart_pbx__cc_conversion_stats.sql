{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p_day',
    alias='cc_conversion_stats',
    engine=ch_engine_merge_tree(),
    order_by='(p_day, result_label)',
    partition_by='toYYYYMM(p_day)',
    settings={'allow_nullable_key': 1},
    tags=['mart_pbx', 'pbx', 'cc'],
  )
}}

- Outcomes of CC requests: one result per entry (dialog_seq = 1).
{% set lookback_days = var('pbx_marts_lookback_days', 3) | int %}

select
  toDate(assumeNotNull(started_at)) as p_day,
  assumeNotNull(result_label) as result_label,
  count() as conversions,
  uniqExact(assign_user_id) as assign_users,
  uniqExact(close_user_id) as close_users
from {{ ref('int_pbx__calls_detail') }}
where result_label is not null
  and result_label != ''
  and dialog_seq = 1
  {% if is_incremental() %}
  and started_at >= today() - {{ lookback_days }}
  {% endif %}
group by p_day, result_label
