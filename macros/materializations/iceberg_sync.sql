{% materialization iceberg_sync, adapter='snowflake' %}
  {% set target_relation = this %}
  {% set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_internal_relation(target_relation) %}
  {% set payload = dbt_snowflake_iceberg_sync.iceberg_sync_config(model, target_relation, internal_relation) %}
  {% do dbt_snowflake_iceberg_sync.iceberg_sync_validate_config(payload) %}
  {% set procedure_relation = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_relation() %}

  {% set existing_relation = adapter.get_relation(
      database=target_relation.database,
      schema=target_relation.schema,
      identifier=target_relation.identifier
    )
  %}

  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {% do adapter.drop_relation(existing_relation) %}
  {% endif %}

  {% call statement('iceberg_sync_call_procedure', fetch_result=True, auto_begin=False) %}
    call {{ procedure_relation }}({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_literal(payload) }})
  {% endcall %}

  {% set result_table = load_result('iceberg_sync_call_procedure') %}
  {% if result_table is none or result_table.get('data') is none or result_table.get('data') | length == 0 %}
    {{ exceptions.raise_compiler_error("iceberg_sync procedure returned no result.") }}
  {% endif %}

  {% set procedure_result = result_table.get('data')[0][0] %}
  {% if procedure_result is string %}
    {% set procedure_result = fromjson(procedure_result) %}
  {% endif %}

  {% if procedure_result is none or procedure_result.get('status') != 'success' %}
    {{ exceptions.raise_compiler_error("iceberg_sync procedure did not return success: " ~ procedure_result) }}
  {% endif %}

  {% set view_columns = procedure_result.get('view_columns', []) %}
  {% do dbt_snowflake_iceberg_sync.iceberg_sync_validate_view_columns(view_columns) %}

  {% call statement('main', auto_begin=False) %}
    create or replace view {{ target_relation }} as
    select
      {% for column in view_columns %}
      {{ adapter.quote(column.get('storage_name')) }} as {{ column.get('alias') }}{% if not loop.last %},{% endif %}
      {% endfor %}
    from {{ internal_relation }}
  {% endcall %}

  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{% materialization iceberg_sync, default %}
  {{ exceptions.raise_compiler_error("iceberg_sync is a Snowflake-only materialization.") }}
{% endmaterialization %}
