{% macro s3_parquet_uri(source, entity) -%}
{{ var('s3_endpoint') }}/{{ var('s3_bucket') }}/bronze/{{ source }}/{{ entity }}/dt={{ var('bronze_dt') }}/run_id=*/data.parquet
{%- endmacro %}

{% macro s3_parquet_run_uri(source, entity) -%}
{%- if execute -%}
  {%- set bronze_run_id = var('bronze_run_id') -%}
{%- else -%}
  {%- set bronze_run_id = var('bronze_run_id', 'documentation') -%}
{%- endif -%}
{{- var('s3_endpoint') }}/{{ var('s3_bucket') }}/bronze/{{ source }}/{{ entity }}/dt={{ var('bronze_dt') }}/run_id={{ bronze_run_id ~ '/*.parquet' }}
{%- endmacro %}

{% macro s3_parquet_all_uri(source, entity) -%}
{{ var('s3_endpoint') }}/{{ var('s3_bucket') }}/bronze/{{ source }}/{{ entity }}/dt=*/run_id=*/data.parquet
{%- endmacro %}

{% macro s3_parquet(source, entity, structure=None) %}
  s3(
    '{{ s3_parquet_uri(source, entity) }}',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    'Parquet'{% if structure %},
    '{{ structure }}'{% endif %}
  )
{% endmacro %}

{% macro s3_parquet_run(source, entity, structure=None) %}
  s3(
    '{{ s3_parquet_run_uri(source, entity) }}',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    'Parquet'{% if structure %},
    '{{ structure }}'{% endif %}
  )
{% endmacro %}

{% macro s3_parquet_all(source, entity, structure=None) %}
  s3(
    '{{ s3_parquet_all_uri(source, entity) }}',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    'Parquet'{% if structure %},
    '{{ structure }}'{% endif %}
  )
{% endmacro %}

{% macro gs_bronze_run_uri(entity) -%}
{%- if execute -%}
  {%- set bronze_run_id = var('bronze_run_id') -%}
{%- else -%}
  {%- set bronze_run_id = var('bronze_run_id', 'documentation') -%}
{%- endif -%}
{{- var('s3_endpoint') }}/{{ var('s3_bucket') }}/bronze/gs/{{ entity }}/dt={{ var('bronze_dt') }}/run_id={{ bronze_run_id }}/*.parquet
{%- endmacro %}

{% macro gs_bronze_s3_read(url, structure=None) %}
(
  SELECT *
  FROM s3(
    '{{ url }}',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    'Parquet'{% if structure is not none %},
    '{{ structure }}'{% endif %}
  )
  SETTINGS use_hive_partitioning = 0
)
{% endmacro %}

{% macro gs_latest_bronze_s3_parquet(entity, structure=None) %}
{% if execute %}
  {% set latest_query %}
    select bronze_run_id, bronze_dt
    from gel_workforce.audit_bronze
    where source = 'gs'
      and entity = '{{ entity }}'
      and coalesce(row_count, 0) > 0
    order by extracted_at desc
    limit 1
  {% endset %}
  {% set latest = run_query(latest_query) %}
  {% if latest and latest.rows | length > 0 %}
    {% set bronze_run_id = latest.rows[0][0] %}
    {% set bronze_dt = latest.rows[0][1] %}
  {% else %}
    {% set bronze_run_id = 'missing' %}
    {% set bronze_dt = '1970-01-01' %}
  {% endif %}
{% else %}
  {% set bronze_run_id = 'documentation' %}
  {% set bronze_dt = '1970-01-01' %}
{% endif %}
  {{ gs_bronze_s3_read(
    var('s3_endpoint') ~ '/' ~ var('s3_bucket') ~ '/bronze/gs/' ~ entity ~ '/dt=' ~ bronze_dt ~ '/run_id=' ~ bronze_run_id ~ '/*.parquet',
    structure
  ) }}
{% endmacro %}

{% macro gs_bronze_parquet(entity, structure=None) %}
  {% if execute %}
    {% if var('bronze_run_id', '') %}
      {{ gs_bronze_s3_read(gs_bronze_run_uri(entity), structure) }}
    {% else %}
      {{ gs_latest_bronze_s3_parquet(entity, structure) }}
    {% endif %}
  {% else %}
    {{ gs_bronze_s3_read(gs_bronze_run_uri(entity), structure) }}
  {% endif %}
{% endmacro %}

{% macro s3_manifest_structure() -%}
source String, entity String, run_id String, bronze_dt Nullable(String), row_count Nullable(Int64), watermark_from Nullable(String), watermark_to Nullable(String), extracted_at Nullable(String), airflow_dag_id Nullable(String), airflow_run_id Nullable(String)
{%- endmacro %}

{% macro s3_manifests_dt(source, entity, dt_glob='*') %}
  s3(
    '{{ var("s3_endpoint") }}/{{ var("s3_bucket") }}/bronze/{{ source }}/{{ entity }}/dt={{ dt_glob }}/run_id=*/manifest.json',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    'JSONEachRow',
    '{{ s3_manifest_structure() }}'
  )
{% endmacro %}

{% macro s3_manifests_all(source, entity) %}
  {{ s3_manifests_dt(source, entity, '*') }}
{% endmacro %}
