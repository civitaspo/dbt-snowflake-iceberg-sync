{% materialization iceberg_sync, adapter='snowflake' %}
  {%- set payload = dbt_snowflake_iceberg_sync.iceberg_sync_collect_config(
    sql, this, model, flags.FULL_REFRESH
  ) -%}
  {%- set procedure_fqn = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_fqn() -%}
  {%- set call_sql -%}
    CALL {{ procedure_fqn }}(
      PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(payload) }})
    )
  {%- endset -%}

  {%- if execute -%}
    {%- set procedure_table = run_query(call_sql) -%}
    {%- set result_values = [] -%}
    {%- if procedure_table is not none and (procedure_table.columns | length) > 0 -%}
      {%- set result_values = procedure_table.columns[0].values() -%}
    {%- endif -%}
    {%- if (result_values | length) == 0 or result_values[0] is none or result_values[0] == '' -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        'procedure returned no result'
      ) -%}
    {%- endif -%}
    {%- set raw_result = result_values[0] -%}
    {%- set procedure_result = dbt_snowflake_iceberg_sync.iceberg_sync_parse_procedure_result(raw_result) -%}
    {%- if procedure_result.get('status') != 'success' -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        procedure_result.get('error_message', 'procedure failed')
      ) -%}
    {%- endif -%}
  {%- endif -%}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
