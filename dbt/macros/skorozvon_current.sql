{% macro dialer_current(model_name) -%}
(select * from {{ ref(model_name) }} final)
{%- endmacro %}
