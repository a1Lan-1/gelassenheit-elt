{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='call_key',
    alias='cur_telephony_calls_l12',
    engine=ch_engine_merge_tree(),
    order_by='(started_at, source, call_key)',
    partition_by='toYYYYMM(started_at)',
    settings={'allow_nullable_key': 1},
    tags=['telephony', 'calls', 'l12'],
  )
}}

{# Single grain call 1l/2l: PBX + Dialer.
   Increment: lookback days by started_at. #}
{% set lookback_days = var('telephony_l12_lookback_days', 3) | int %}

with pbx_l12 as (
  select
    'pbx' as source,
    assumeNotNull(entry_id) as call_key,
    entry_id as entry_id,
    toUInt32(ifNull(dialog_seq, 1)) as dialog_seq,
    assumeNotNull(started_at) as started_at,
    call_type_code as call_type_code,
    toUInt8(ifNull(missed_flag, 0)) as missed_flag,
    toUInt8(ifNull(is_real_transfer_hop, 0)) as is_real_transfer_hop,
    toUInt8(ifNull(is_consultation, 0)) as is_consultation,
    toFloat64(wait_to_accept_sec) as wait_to_accept_sec,
    toFloat64(aht_talk_duration) as aht_talk_duration,
    toFloat64(asa_sec) as asa_sec,
    toString(user_id) as user_id,
    user_name as user_name,
    replaceRegexpAll(ifNull(client_phone, ''), '[^0-9]', '') as client_phone,
    group_name as source_group_name,
    multiIf(
      group_name = 'L1 inbound calls', 'L1 Support',
      group_name = 'L2 technical support', 'L2 Support',
      CAST(NULL AS Nullable(String))
    ) as user_group_name,
    multiIf(
      group_name = 'L1 inbound calls', 'l1',
      group_name = 'L2 technical support', 'l2',
      CAST(NULL AS Nullable(String))
    ) as line_code
  from {{ ref('int_pbx__calls_detail') }}
  where group_name in ('L1 inbound calls', 'L2 technical support')
  {% if is_incremental() %}
    and started_at >= today() - {{ lookback_days }}
  {% endif %}
),

sz_hours as (
  select
    toUInt64(user_id) as user_id,
    any(full_name) as full_name,
    any(email) as email
  from {{ ref('int_dialer__hours_current') }}
  final
  where user_id is not null and user_id != 0
  group by user_id
),

fsd_users as (
  select
    id,
    lowerUTF8(trimBoth(coalesce(email, ''))) as email,
    lowerUTF8(trimBoth(concat(
      coalesce(last_name, ''), ' ',
      coalesce(first_name, ''), ' ',
      coalesce(middle_name, '')
    ))) as fio
  from {{ ref('int_helpdesk__users_current') }}
  final
  where deleted_at is null
),

fsd_resp as (
  select
    user_id,
    argMax(user_group_id, updated_at) as user_group_id
  from {{ ref('int_helpdesk__user_groups_mapping_current') }}
  final
  where deleted_at is null
  group by user_id
),

sz_l12 as (
  select
    'dialer' as source,
    concat('sz-', toString(c.id)) as call_key,
    concat('sz-', toString(c.id)) as entry_id,
    toUInt32(1) as dialog_seq,
    assumeNotNull(c.started_at) as started_at,
    c.call_type_code as call_type_code,
    toUInt8(c.call_type_code = 'missed') as missed_flag,
    -- SZ transfered = one hop of translation, not missed or consult-split
    toUInt8(c.call_type_code = 'transfered') as is_real_transfer_hop,
    toUInt8(0) as is_consultation,
    toFloat64(coalesce(c.waiting_on_line_time, 0)) as wait_to_accept_sec,
    toFloat64(if(
      c.call_type_code in ('incoming', 'transfered'),
      coalesce(c.duration, 0),
      NULL
    )) as aht_talk_duration,
    CAST(NULL AS Nullable(Float64)) as asa_sec,
    toString(c.user_id) as user_id,
    c.user_name as user_name,
    replaceRegexpAll(ifNull(c.client_phone, ''), '[^0-9]', '') as client_phone,
    g.name as source_group_name,
    g.name as user_group_name,
    multiIf(
      g.name = 'L1 Support', 'l1',
      g.name = 'L2 Support', 'l2',
      CAST(NULL AS Nullable(String))
    ) as line_code
  from {{ ref('int_dialer__calls_detail') }} as c
  left join sz_hours as h on h.user_id = toUInt64(c.user_id)
  left join fsd_users as fu_e
    on fu_e.email != ''
    and fu_e.email = lowerUTF8(trimBoth(coalesce(h.email, '')))
  left join fsd_users as fu_f
    on fu_f.fio = lowerUTF8(trimBoth(coalesce(h.full_name, c.user_name)))
  left join fsd_resp as r on r.user_id = coalesce(fu_e.id, fu_f.id)
  left join {{ ref('int_helpdesk__user_groups_current') }} as g
    final on g.id = r.user_group_id
  where g.name in ('L1 Support', 'L2 Support')
  {% if is_incremental() %}
    and c.started_at >= today() - {{ lookback_days }}
  {% endif %}
)

select * from pbx_l12
union all
select * from sz_l12
