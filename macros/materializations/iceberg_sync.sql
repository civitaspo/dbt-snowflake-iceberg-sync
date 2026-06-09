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
  {%- set retry_config = payload['retry'] -%}
  {%- set retry_max_attempts = retry_config.get('max_attempts', 3) -%}
  {%- set retry_initial_delay_seconds = retry_config.get('initial_delay_seconds', 5) -%}
  {%- set retry_max_delay_seconds = retry_config.get('max_delay_seconds', 60) -%}
  {%- set retry_backoff_multiplier = retry_config.get('backoff_multiplier', 2.0) -%}
  {%- set retry_call_block -%}
    DECLARE
      iceberg_sync_attempt INTEGER DEFAULT 1;
      iceberg_sync_max_attempts INTEGER DEFAULT {{ retry_max_attempts }};
      iceberg_sync_initial_delay_seconds FLOAT DEFAULT {{ retry_initial_delay_seconds }};
      iceberg_sync_max_delay_seconds FLOAT DEFAULT {{ retry_max_delay_seconds }};
      iceberg_sync_backoff_multiplier FLOAT DEFAULT {{ retry_backoff_multiplier }};
      iceberg_sync_result VARIANT;
      iceberg_sync_message STRING;
      iceberg_sync_delay_seconds FLOAT;
      iceberg_sync_delay_milliseconds INTEGER;
    BEGIN
      WHILE (iceberg_sync_attempt <= iceberg_sync_max_attempts) DO
        BEGIN
          {{ call_sql }} INTO :iceberg_sync_result;
          RETURN iceberg_sync_result;
        EXCEPTION
          WHEN OTHER THEN
            iceberg_sync_message := SQLERRM;
            IF (
              iceberg_sync_attempt < iceberg_sync_max_attempts
              AND (
                POSITION('sql execution internal error' IN LOWER(iceberg_sync_message)) > 0
                OR POSITION('incident' IN LOWER(iceberg_sync_message)) > 0
                OR POSITION(
                  'scoped transaction started in stored procedure is incomplete'
                  IN LOWER(iceberg_sync_message)
                ) > 0
              )
            ) THEN
              iceberg_sync_delay_seconds := LEAST(
                iceberg_sync_max_delay_seconds,
                iceberg_sync_initial_delay_seconds
                  * POWER(iceberg_sync_backoff_multiplier, iceberg_sync_attempt - 1)
              );
              iceberg_sync_delay_milliseconds := CEIL(iceberg_sync_delay_seconds * 1000);
              IF (iceberg_sync_delay_milliseconds > 0) THEN
                CALL SYSTEM$WAIT(:iceberg_sync_delay_milliseconds, 'MILLISECONDS');
              END IF;
              iceberg_sync_attempt := iceberg_sync_attempt + 1;
            ELSE
              RAISE;
            END IF;
        END;
      END WHILE;
    END
  {%- endset -%}
  {%- set retry_call_sql = "EXECUTE IMMEDIATE " ~ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(retry_call_block) -%}

  {%- if execute -%}
    {%- call statement('main', fetch_result=True) -%}
      {{ retry_call_sql }}
    {%- endcall -%}
    {%- set procedure_table = load_result('main')['table'] -%}
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
