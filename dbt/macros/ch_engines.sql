{% macro ch_zk_path() -%}
/clickhouse/tables/{shard}/{database}/{table}
{%- endmacro %}

{# sync_* on s-ch00: ENGINE=Replicated database → ReplicatedMergeTree() without zk/replica args.
   Set DWH_CH_REPLICATED_DB=true on sd-anl-dbt00 (default). #}
{% macro ch_replicated_database() -%}
{{- env_var('DWH_CH_REPLICATED_DB', 'true') | lower in ['1', 'true', 'yes'] -}}
{%- endmacro %}

{% macro ch_engine_merge_tree() -%}
{%- if ch_replicated_database() -%}
ReplicatedMergeTree()
{%- else -%}
ReplicatedMergeTree('{{ ch_zk_path() }}', '{replica}')
{%- endif -%}
{%- endmacro %}

{% macro ch_engine_replacing(version_col=none) -%}
{%- if ch_replicated_database() -%}
{%- if version_col is not none -%}
ReplicatedReplacingMergeTree({{ version_col }})
{%- else -%}
ReplicatedReplacingMergeTree()
{%- endif -%}
{%- else -%}
{%- if version_col is not none -%}
ReplicatedReplacingMergeTree('{{ ch_zk_path() }}', '{replica}', {{ version_col }})
{%- else -%}
ReplicatedReplacingMergeTree('{{ ch_zk_path() }}', '{replica}')
{%- endif -%}
{%- endif -%}
{%- endmacro %}
