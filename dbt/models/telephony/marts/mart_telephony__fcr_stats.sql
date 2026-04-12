{{
  config(
    materialized='table',
    alias='mart_telephony_fcr_stats',
    engine=ch_engine_merge_tree(),
    order_by='(date_call, user_group_name)',
    partition_by='toYYYYMM(date_call)',
    settings={'allow_nullable_key': 1},
    tags=['mart_telephony', 'telephony', 'fcr'],
  )
}}

{# FCR by unified incoming 1l/2l (window 24/72/168h, like pbx/dialer v_fcr_stats). #}

with calls as (
  select
    started_at,
    client_phone,
    user_group_name,
    line_code,
    toNullable(started_at) as started_at_n
  from {{ ref('int_telephony__calls_l12') }}
  where call_type_code = 'incoming'
    and client_phone is not null
    and client_phone != ''
    and user_group_name is not null
    and user_group_name != ''
),

with_next as (
  select
    toDate(started_at) as date_call,
    user_group_name,
    line_code,
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
    user_group_name,
    line_code,
    if(next_at is null or dateDiff('second', started_at, next_at) > 86400, 1, 0) as fcr_24,
    if(next_at is null or dateDiff('second', started_at, next_at) > 259200, 1, 0) as fcr_72,
    if(next_at is null or dateDiff('second', started_at, next_at) > 604800, 1, 0) as fcr_168
  from with_next
)

select
  date_call,
  user_group_name,
  any(line_code) as line_code,
  sum(fcr_24) / count() as fcr_24_percent,
  sum(fcr_72) / count() as fcr_72_percent,
  sum(fcr_168) / count() as fcr_168_percent,
  count() as incoming_with_phone
from stats
group by date_call, user_group_name
