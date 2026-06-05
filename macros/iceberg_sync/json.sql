{% macro iceberg_sync_json_sql_literal(value) -%}
  {%- set json_value = value | tojson -%}
  {%- set json_value = json_value | replace("\\u0027", "'") -%}
  {%- set escaped = json_value | replace("\\", "\\\\") | replace("'", "''") -%}
  {{ return("'" ~ escaped ~ "'") }}
{%- endmacro %}

{% macro iceberg_sync_parse_procedure_result(raw_result) -%}
  {%- if raw_result is string -%}
    {{ return(fromjson(raw_result)) }}
  {%- else -%}
    {{ return(raw_result) }}
  {%- endif -%}
{%- endmacro %}
