{% macro sql_string_literal(value) %}
  {{ return("'" ~ (value | string | replace("'", "''")) ~ "'") }}
{% endmacro %}

{% macro iceberg_sync_json_literal(value) %}
  {{ return("parse_json(" ~ dbt_snowflake_iceberg_sync.sql_string_literal(tojson(value)) ~ ")") }}
{% endmacro %}
