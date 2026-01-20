{% macro drop_dbt_orphan_suffixes(relation) %}

  {# dbt-clickhouse swap tables; adapter.drop_relation fails when get_relation type is null. #}

  {% for suffix in ['__dbt_tmp', '__dbt_backup', '__dbt_new_data'] %}

    {% set orphan_id = relation.identifier ~ suffix %}

    {% set fq = adapter.quote(relation.schema) ~ '.' ~ adapter.quote(orphan_id) %}

    {% do run_query('DROP TABLE IF EXISTS ' ~ fq) %}

  {% endfor %}

{% endmacro %}



{% macro drop_dbt_tmp_suffix(relation) %}

  {{ drop_dbt_orphan_suffixes(relation) }}

{% endmacro %}

