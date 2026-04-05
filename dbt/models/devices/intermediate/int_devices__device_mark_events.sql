{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='esp_device_mark_events',
    engine=ch_engine_replacing('_dbt_loaded_at'),
    order_by='(cashdesk_id)',
    unique_key=['cashdesk_id'],
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['esp', 'mart_esp', 'license_funnel', 'install_report'],
  )
}}

{#
  Grain: one row per cashdesk_id (first Local/Online marks).
  Incremental: min over lookback window + least with stored first_*.
  Full min() over history only on full-refresh.
#}
{% set lookback_days = var('device_mark_events_lookback_days', 2) | int %}

with touched as (
  select distinct cashdesk_id
  from {{ ref('int_devices__device_mark_daily') }}
  {% if is_incremental() %}
  where event_day >= today() - {{ lookback_days }}
  {% endif %}
),

window_agg as (
  select
    cashdesk_id,
    min(min_event_at) as window_first_mark_at,
    nullIf(
      minIf(min_event_at, type = 'imcGismtDataOnline'),
      toDateTime64('1970-01-01 00:00:00', 3)
    ) as window_first_online_mark_at
  from {{ ref('int_devices__device_mark_daily') }}
  where cashdesk_id in (select cashdesk_id from touched)
  {% if is_incremental() %}
    and event_day >= today() - {{ lookback_days }}
  {% endif %}
  group by cashdesk_id
)

{% if is_incremental() %}
,
prev as (
  select
    cashdesk_id,
    first_mark_at,
    first_online_mark_at
  from {{ this }}
  where cashdesk_id in (select cashdesk_id from touched)
  order by cashdesk_id, _dbt_loaded_at desc
  limit 1 by cashdesk_id
)
{% endif %}

select
  w.cashdesk_id as cashdesk_id,
  {% if is_incremental() %}
  if(
    p.first_mark_at is null,
    w.window_first_mark_at,
    least(p.first_mark_at, w.window_first_mark_at)
  ) as first_mark_at,
  multiIf(
    p.first_online_mark_at is null and w.window_first_online_mark_at is null,
    CAST(NULL, 'Nullable(DateTime64(3))'),
    p.first_online_mark_at is null,
    w.window_first_online_mark_at,
    w.window_first_online_mark_at is null,
    p.first_online_mark_at,
    least(p.first_online_mark_at, w.window_first_online_mark_at)
  ) as first_online_mark_at,
  {% else %}
  w.window_first_mark_at as first_mark_at,
  w.window_first_online_mark_at as first_online_mark_at,
  {% endif %}
  now64(3) as _dbt_loaded_at
from window_agg as w
{% if is_incremental() %}
left join prev as p on p.cashdesk_id = w.cashdesk_id
{% endif %}
