{% macro iceberg_sync_normalize_object_identifier(value) -%}
  {{ return((value | string | trim | replace('"', '') | upper)) }}
{%- endmacro %}

{% macro iceberg_sync_internal_identifier(target_relation) -%}
  {{ return(dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
    "__" ~ target_relation.identifier
  )) }}
{%- endmacro %}

{% macro iceberg_sync_relation_payload(relation) -%}
  {{ return({
    'database': dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      relation.database
    ),
    'schema': dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      relation.schema
    ),
    'identifier': dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      relation.identifier
    )
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
    'database': dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      parts[0]
    ),
    'schema': dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      parts[1]
    ),
    'identifier': dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      parts[2]
    )
  }) }}
{%- endmacro %}
