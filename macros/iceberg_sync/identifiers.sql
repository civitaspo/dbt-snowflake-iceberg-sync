{% macro iceberg_sync_internal_identifier(target_relation) -%}
  {{ return("__" ~ target_relation.identifier) }}
{%- endmacro %}

{% macro iceberg_sync_relation_payload(relation) -%}
  {{ return({
    'database': relation.database,
    'schema': relation.schema,
    'identifier': relation.identifier
  }) }}
{%- endmacro %}

{% macro iceberg_sync_relation_from_fqn(value, field_name) -%}
  {%- set parts = (value | string).split('.') -%}
  {%- if parts | length != 3 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      field_name ~ " must be a three-part relation name"
    ) -%}
  {%- endif -%}
  {{ return({
    'database': parts[0],
    'schema': parts[1],
    'identifier': parts[2]
  }) }}
{%- endmacro %}
