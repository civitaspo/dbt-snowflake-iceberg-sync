{% materialization iceberg_sync, adapter='snowflake' %}
  {%- set payload = dbt_snowflake_iceberg_sync.iceberg_sync_collect_config(
    sql, this, model, flags.FULL_REFRESH
  ) -%}
  {%- set procedure_relation = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_relation() -%}
  {%- set call_sql -%}
    CALL {{ procedure_relation }}(
      PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(payload) }})
    )
  {%- endset -%}

  {%- if execute -%}
    {%- set procedure_table = run_query(call_sql) -%}
    {%- set raw_result = procedure_table.columns[0].values()[0] -%}
    {%- set procedure_result = dbt_snowflake_iceberg_sync.iceberg_sync_parse_procedure_result(raw_result) -%}
    {%- if procedure_result.get('status') != 'success' -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        procedure_result.get('error_message', 'procedure failed')
      ) -%}
    {%- endif -%}
    {%- set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
      procedure_result['internal_relation'], 'table'
    ) -%}
    {%- call statement('main') -%}
      {{ dbt_snowflake_iceberg_sync.iceberg_sync_create_view_sql(
        this, internal_relation, procedure_result['view_columns']
      ) }}
    {%- endcall -%}
  {%- endif -%}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
