{% macro install_iceberg_sync_procedure() %}
  {% set deployment = var('iceberg_sync', {}) %}
  {% set procedure_relation = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_relation() %}
  {% set handler_stage = dbt_snowflake_iceberg_sync.iceberg_sync_unstage(dbt_snowflake_iceberg_sync.iceberg_sync_required_var(deployment, 'handler_stage')) %}
  {% set handler_stage_path = deployment.get('handler_stage_path', 'procedure') %}
  {% set handler_import_name = deployment.get('handler_import_name', 'iceberg_sync_procedure') %}
  {% set handler_name = deployment.get('handler_name', handler_import_name ~ '.handler.main') %}
  {% set handler_local_path = dbt_snowflake_iceberg_sync.iceberg_sync_required_var(deployment, 'handler_local_path') %}
  {% set external_access_integrations = deployment.get('external_access_integrations', []) %}
  {% set gcp_auth_method = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_auth_method', 'service_account_key') %}
  {% if gcp_auth_method not in ['service_account_key', 'workload_identity_federation'] %}
    {{ exceptions.raise_compiler_error("vars.iceberg_sync.gcp_auth_method (or top-level var iceberg_sync_gcp_auth_method) must be 'service_account_key' or 'workload_identity_federation'.") }}
  {% endif %}
  {% if gcp_auth_method == 'service_account_key' %}
    {% set gcp_sa_secret_fqdn = dbt_snowflake_iceberg_sync.iceberg_sync_required_var(deployment, 'gcp_sa_secret_fqdn') %}
    {% set gcp_sa_secret_alias = deployment.get('gcp_sa_secret_alias', 'gcp_sa_credentials_json') %}
  {% else %}
    {# WIF tokens are issued per run with SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN,
       so no secrets clause is bound to the procedure. Require the per-run values here
       anyway to fail fast at install time. #}
    {% do dbt_snowflake_iceberg_sync.iceberg_sync_required_deployment_var(deployment, 'gcp_wif_secret_fqdn') %}
    {% do dbt_snowflake_iceberg_sync.iceberg_sync_required_deployment_var(deployment, 'gcp_wif_audience') %}
  {% endif %}

  {% call statement('iceberg_sync_create_handler_stage', auto_begin=False) %}
    create stage if not exists {{ handler_stage }}
  {% endcall %}

  {% call statement('iceberg_sync_remove_handler_stage_files', auto_begin=False) %}
    remove @{{ handler_stage }}/{{ handler_stage_path }}
  {% endcall %}

  {% call statement('iceberg_sync_put_handler_root', auto_begin=False) %}
    put file://{{ handler_local_path }}/*.py @{{ handler_stage }}/{{ handler_stage_path }}/ auto_compress=false overwrite=true
  {% endcall %}

  {% call statement('iceberg_sync_put_handler_sources', auto_begin=False) %}
    put file://{{ handler_local_path }}/sources/*.py @{{ handler_stage }}/{{ handler_stage_path }}/sources/ auto_compress=false overwrite=true
  {% endcall %}

  {% call statement('iceberg_sync_create_procedure', auto_begin=False) %}
    create or replace procedure {{ procedure_relation }}(config variant)
    returns variant
    language python
    runtime_version = '3.12'
    packages = ('snowflake-snowpark-python', 'requests', 'google-auth')
    imports = ('@{{ handler_stage }}/{{ handler_stage_path }}/={{ handler_import_name }}/')
    handler = '{{ handler_name }}'
    {% if external_access_integrations | length > 0 %}
    external_access_integrations = ({{ external_access_integrations | join(', ') }})
    {% endif %}
    {% if gcp_auth_method == 'service_account_key' %}
    secrets = ('{{ gcp_sa_secret_alias }}' = {{ gcp_sa_secret_fqdn }})
    {% endif %}
    execute as caller
  {% endcall %}

  {{ log("Installed iceberg_sync procedure at " ~ procedure_relation, info=true) }}
{% endmacro %}
