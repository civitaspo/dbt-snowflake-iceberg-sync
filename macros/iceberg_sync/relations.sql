{% macro iceberg_sync_internal_relation(target_relation) %}
  {{ return(api.Relation.create(
      database=target_relation.database,
      schema=target_relation.schema,
      identifier='__' ~ target_relation.identifier,
      type='table'
    ))
  }}
{% endmacro %}

{% macro iceberg_sync_relation_dict(relation) %}
  {{ return({
      'database': relation.database,
      'schema': relation.schema,
      'identifier': relation.identifier,
      'rendered': relation | string
    })
  }}
{% endmacro %}

{% macro iceberg_sync_procedure_relation() %}
  {% set deployment = var('iceberg_sync', {}) %}
  {% set procedure_database = dbt_snowflake_iceberg_sync.iceberg_sync_required_var(deployment, 'procedure_database') %}
  {% set procedure_schema = dbt_snowflake_iceberg_sync.iceberg_sync_required_var(deployment, 'procedure_schema') %}
  {% set procedure_name = dbt_snowflake_iceberg_sync.iceberg_sync_required_var(deployment, 'procedure_name') %}
  {{ return(api.Relation.create(
      database=procedure_database,
      schema=procedure_schema,
      identifier=procedure_name,
      type='procedure'
    ))
  }}
{% endmacro %}
