{% macro bronze_manifest_select(source_timezone='MSK') %}
  source,
  entity,
  run_id as bronze_run_id,
  bronze_dt,
  row_count,
  {{ ch_manifest_datetime('watermark_from', source_timezone) }} as watermark_from,
  {{ ch_manifest_datetime('watermark_to', source_timezone) }} as watermark_to,
  {{ ch_manifest_datetime('extracted_at', source_timezone) }} as extracted_at,
  airflow_dag_id,
  airflow_run_id,
  _path as manifest_path
{% endmacro %}

{% macro bronze_manifest_row(source, entity, source_timezone='MSK', dt_glob='*') %}
select
  {{ bronze_manifest_select(source_timezone) }}
from {{ s3_manifests_dt(source, entity, dt_glob) }}
{% endmacro %}

{% macro s3_manifests_source_dt(source, dt_glob='*') %}
  s3(
    '{{ var("s3_endpoint") }}/{{ var("s3_bucket") }}/bronze/{{ source }}/*/dt={{ dt_glob }}/run_id=*/manifest.json',
    '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    'JSONEachRow',
    '{{ s3_manifest_structure() }}'
  )
{% endmacro %}

{% macro bronze_manifests_union(source, entities, source_timezone='MSK') %}
{#-
  Backfill-only helper for audit_bronze dbt models.
  Primary path: Airflow ingest INSERT after bronze upload (no S3 scan).

  Full refresh: one S3 glob per source (all entities, all dates).
  Incremental: one S3 glob per lookback day for the whole source, then filter entities
  and anti-join against already loaded keys.
-#}
{% set lookback_days = var('audit_bronze_lookback_days', 2) | int %}
{% if is_incremental() %}
  {% set base_dt = modules.datetime.datetime.strptime(var('bronze_dt') | string, '%Y-%m-%d').date() %}
  {% set dt_globs = [] %}
  {% for i in range(lookback_days) %}
    {% set _ = dt_globs.append((base_dt - modules.datetime.timedelta(days=i)).isoformat()) %}
  {% endfor %}
{% else %}
  {% set dt_globs = ['*'] %}
{% endif %}

select *
from (
{% set ns = namespace(first=true) %}
{% for dt_glob in dt_globs %}
{% if not ns.first %}
union all
{% endif %}
{% set ns.first = false %}
select
  {{ bronze_manifest_select(source_timezone) }}
from {{ s3_manifests_source_dt(source, dt_glob) }}
where entity in (
{% for entity in entities %}
  '{{ entity }}'{% if not loop.last %},{% endif %}
{% endfor %}
)
{% endfor %}
) as manifests
{% if is_incremental() %}
where (source, entity, bronze_run_id) not in (
  select source, entity, bronze_run_id
  from {{ this }}
)
{% endif %}
{% endmacro %}
