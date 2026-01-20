{% macro fsd_current(model_name) -%}
(select * from {{ ref(model_name) }} final)
{%- endmacro %}

{% macro fsd_history_current() -%}
{{ fsd_current('int_helpdesk__history_current') }}
{%- endmacro %}
