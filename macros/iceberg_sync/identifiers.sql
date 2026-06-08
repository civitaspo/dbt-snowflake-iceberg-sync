{% macro iceberg_sync_normalize_object_identifier(value) -%}
  {{ return((value | string | trim | replace('"', '') | upper)) }}
{%- endmacro %}

{% macro iceberg_sync_quote_object_identifier(value) -%}
  {{ return('"' ~ (
    dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(value)
    | replace('"', '""')
  ) ~ '"') }}
{%- endmacro %}

{% macro iceberg_sync_object_fqn(value, field_name, min_parts=1, max_parts=3) -%}
  {%- set parts = (value | string).split('.') -%}
  {%- if parts | length < min_parts or parts | length > max_parts -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      field_name ~ " must have between " ~ min_parts ~ " and " ~ max_parts ~ " parts"
    ) -%}
  {%- endif -%}
  {%- set quoted_parts = [] -%}
  {%- for part in parts -%}
    {%- set normalized = dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
      part
    ) -%}
    {%- if normalized == "" -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        field_name ~ " contains an empty identifier"
      ) -%}
    {%- endif -%}
    {%- do quoted_parts.append(
      dbt_snowflake_iceberg_sync.iceberg_sync_quote_object_identifier(normalized)
    ) -%}
  {%- endfor -%}
  {{ return(quoted_parts | join('.')) }}
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
