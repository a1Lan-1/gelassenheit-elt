{% macro fsd_json_str(col, key) -%}
{# CH 26.2: JSON path needs unqualified col.key (table_alias.col.key fails). #}
ifNull(toString({{ col }}.`{{ key }}`), '')
{%- endmacro %}

{% macro fsd_json_notify_label(col) -%}
{# notify is a JSON *string* containing an array, not Array(JSON). #}
coalesce(
  nullIf(JSONExtractString(toString({{ col }}.notify), 1, 'label'), ''),
  nullIf(JSONExtractString(toString({{ col }}.notify), 1, 'name'), ''),
  ''
)
{%- endmacro %}

{% macro fsd_change_json_typed(old_expr, new_expr, old_key, new_key) -%}
if(
  ({{ old_expr }}) != '' or ({{ new_expr }}) != '',
  accurateCastOrNull(
    concat(
      '{"{{ old_key }}":', toJSONString(if(({{ old_expr }}) = '', 'null', {{ old_expr }})),
      ',"{{ new_key }}":', toJSONString(if(({{ new_expr }}) = '', 'null', {{ new_expr }})), '}'
    ),
    'JSON'
  ),
  CAST(NULL, 'Nullable(JSON)')
)
{%- endmacro %}

{% macro fsd_change_json(old_expr, new_expr) -%}
{{ fsd_change_json_typed(old_expr, new_expr, 'old', 'new') }}
{%- endmacro %}
