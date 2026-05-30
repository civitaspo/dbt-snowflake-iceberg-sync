{% macro iceberg_sync_relation_from_payload(payload, relation_type='table') -%}
  {{ return(api.Relation.create(
    database=payload['database'],
    schema=payload['schema'],
    identifier=payload['identifier'],
    type=relation_type,
    quote_policy={'database': true, 'schema': true, 'identifier': true}
  )) }}
{%- endmacro %}

{% macro iceberg_sync_create_view_sql(target_relation, internal_relation, view_columns) -%}
  CREATE OR REPLACE VIEW {{ target_relation }} AS
  SELECT
  {%- for column in view_columns %}
    {{ adapter.quote(column['source_name']) }} AS {{ column['alias'] }}{{ "," if not loop.last }}
  {%- endfor %}
  FROM {{ internal_relation }}
{%- endmacro %}
