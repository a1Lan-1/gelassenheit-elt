{% macro bronze_parquet(source, entity, structure=None) %}
  {% if flags.FULL_REFRESH %}
    {{ s3_parquet_all(source, entity, structure) }}
  {% else %}
    {{ s3_parquet_run(source, entity, structure) }}
  {% endif %}
{% endmacro %}
