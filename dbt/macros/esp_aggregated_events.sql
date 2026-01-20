{% macro esp_event_types_list(event_types) %}
  {%- for t in event_types -%}
    '{{ t }}'{% if not loop.last %}, {% endif %}
  {%- endfor -%}
{% endmacro %}

{% macro esp_aggregated_events_filtered(event_types, has_error=false) %}
(
  select
    event_day,
    cashdesk_id::UInt64 as cashdesk_id,
    type,
    has_error,
    min_event_date_in_day,
    max_event_date_in_day,
    events_count_in_day
  from {{ source('anl', 'aggregated_events') }}
  where cashdesk_id > 0
    and has_error = {{ 'true' if has_error else 'false' }}
    and type in ({{ esp_event_types_list(event_types) }})
)
{% endmacro %}
