{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date_call', 'user_name'],
    alias='v_fcr_by_operators_stats',
    engine=ch_engine_merge_tree(),
    order_by='(date_call, user_name)',
    settings={'allow_nullable_key': 1},
    tags=['mart_dialer', 'dialer', 'fcr'],
  )
}}

{% set lookback_days = var('dialer_fcr_lookback_days', 8) | int %}

with calls as (
  select
    started_at,
    client_phone,
    user_name,
    toNullable(started_at) as started_at_n
  from {{ ref('int_dialer__calls_detail') }}
  where call_type_code = 'incoming'
    and client_phone is not null
    and client_phone != ''
  {% if is_incremental() %}
    and started_at >= today() - {{ lookback_days }}
  {% endif %}
),

with_next as (
  select
    toDate(started_at) as date_call,
    client_phone,
    user_name,
    started_at,
    leadInFrame(started_at_n, 1) over (
      partition by client_phone
      order by started_at
      rows between unbounded preceding and unbounded following
    ) as next_at
  from calls
),

stats as (
  select
    date_call,
    user_name,
    if(next_at is null or dateDiff('second', started_at, next_at) > 86400, 1, 0) as fcr_24,
    if(next_at is null or dateDiff('second', started_at, next_at) > 259200, 1, 0) as fcr_72,
    if(next_at is null or dateDiff('second', started_at, next_at) > 604800, 1, 0) as fcr_168
  from with_next
)

select
  date_call,
  user_name,
  sum(fcr_24) / count() as fcr_24_percent,
  sum(fcr_72) / count() as fcr_72_percent,
  sum(fcr_168) / count() as fcr_168_percent
from stats
group by date_call, user_name
