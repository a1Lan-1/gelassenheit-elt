{# ClickHouse 26.2 JSON accepts objects only — top-level arrays fail cast and become NULL.
   Wrap arrays as {"items":[...]}; objects pass through unchanged.
   Read array payload via JSONExtractArrayRaw(toString(col), 'items') or toString(col).
   Never rely on bare CAST(array AS JSON): ReplacingMergeTree will overwrite good rows with NULL. #}
{% macro ch_json_from_string(expr) -%}
if(
  isValidJSON({{ expr }}),
  accurateCastOrNull(
    if(
      startsWith(ltrim({{ expr }}), '['),
      concat('{"items":', {{ expr }}, '}'),
      {{ expr }}
    ),
    'JSON'
  ),
  NULL
)
{%- endmacro %}
