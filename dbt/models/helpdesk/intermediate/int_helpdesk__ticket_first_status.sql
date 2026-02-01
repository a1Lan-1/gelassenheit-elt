{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    alias='cur_ticket_first_status',
    engine=ch_engine_replacing('_dbt_loaded_at'),
    order_by='(ticket_id)',
    unique_key=['ticket_id'],
    settings={'allow_nullable_key': 1},
    post_hook="{{ drop_dbt_tmp_suffix(this) }}",
    tags=['helpdesk', 'current'],
  )
}}

{# The first ticket status is unchanged: increment — only new ticket_id for lookback. #}
{% set lookback_days = var('ticket_first_status_lookback_days', var('operational_load_lookback_days', 3)) | int %}

with
{% if is_incremental() %}
candidate_tickets as (
  select distinct object_id as ticket_id
  from {{ ref('int_helpdesk__history_current') }}
  where object_type = 'tickets'
    and updated_at >= today() - {{ lookback_days }}
    and ifNull(toString(new_values.status_id), '') != ''
    and object_id not in (
      select ticket_id from {{ this }}
    )
),
{% endif %}

history_typed as (
  select
    object_id as ticket_id,
    updated_at as updated_at,
    ifNull(toString(new_values.status_id), '') as status_id
  from {{ ref('int_helpdesk__history_current') }}
  {% if is_incremental() %}
  prewhere object_id in (select ticket_id from candidate_tickets)
  {% else %}
  final
  {% endif %}
  where object_type = 'tickets'
    and ifNull(toString(new_values.status_id), '') != ''
),

history_ranked as (
  select
    h.ticket_id as ticket_id,
    st.name as first_status_name,
    row_number() over (partition by h.ticket_id order by h.updated_at) as rn
  from history_typed as h
  inner join {{ fsd_current('int_helpdesk__tickets_current') }} as t
    on t.id = h.ticket_id
    and t.deleted_at is null
  left join (
    select id, name
    from {{ fsd_current('int_helpdesk__statuses_current') }}
  ) as st on toString(st.id) = h.status_id
)

select
  ticket_id,
  first_status_name,
  now() as _dbt_loaded_at
from history_ranked
where rn = 1
