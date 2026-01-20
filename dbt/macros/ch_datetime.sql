{% macro ch_parse_msk_datetime(expr, precision=3) -%}
parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString({{ expr }})), ''), {{ precision }})
{%- endmacro %}

{% macro ch_shift_utc_to_msk(expr) -%}
if(isNull({{ expr }}), NULL, {{ expr }} + toIntervalHour(3))
{%- endmacro %}

{% macro ch_parse_datetime64_or_null(expr, precision=3) -%}
parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString({{ expr }})), ''), {{ precision }})
{%- endmacro %}

{% macro ch_parse_deleted_at_or_null(expr, precision=3) -%}
{%- set epoch = "toDateTime64('1970-01-01 00:00:00', " ~ precision ~ ")" -%}
nullIf(
  parseDateTime64BestEffortOrNull(
    nullIf(
      nullIf(
        nullIf(
          nullIf(
            nullIf(trimBoth(toString({{ expr }})), ''),
            '1970-01-01'
          ),
          '1970-01-01 00:00:00'
        ),
        '1970-01-01T00:00:00'
      ),
      '1970-01-01T00:00:00.000'
    ),
    {{ precision }}
  ),
  {{ epoch }}
)
{%- endmacro %}

{% macro ch_cast_deleted_at_or_null(expr, precision=3) -%}
CAST({{ ch_parse_deleted_at_or_null(expr, precision) }}, 'Nullable(DateTime64({{ precision }}))')
{%- endmacro %}

{% macro ch_manifest_datetime(expr, source_timezone='MSK', precision=3) -%}
{%- set parsed = 'parseDateTime64BestEffortOrNull(nullIf(trimBoth(toString(' ~ expr ~ ')), \'\'), ' ~ precision ~ ')' -%}
{%- set tz = (source_timezone or 'MSK') | upper -%}
{%- if tz in ['UTC', 'GMT', 'ETC/UTC'] -%}
CAST({{ ch_shift_utc_to_msk(parsed) }}, 'Nullable(DateTime64({{ precision }}))')
{%- else -%}
CAST({{ parsed }}, 'Nullable(DateTime64({{ precision }}))')
{%- endif -%}
{%- endmacro %}
