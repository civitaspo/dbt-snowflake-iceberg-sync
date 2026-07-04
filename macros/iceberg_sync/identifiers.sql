{% macro iceberg_sync_quote_identifier(identifier) %}
  {{ return('"' ~ (identifier | string | replace('"', '""')) ~ '"') }}
{% endmacro %}

{% macro iceberg_sync_unstage(stage_name) %}
  {% set clean = stage_name | string %}
  {% if clean.startswith('@') %}
    {% set clean = clean[1:] %}
  {% endif %}
  {{ return(clean) }}
{% endmacro %}

{% macro iceberg_sync_stage_reference(stage_name) %}
  {{ return('@' ~ dbt_snowflake_iceberg_sync.iceberg_sync_unstage(stage_name)) }}
{% endmacro %}

{% macro iceberg_sync_required_var(config, key) %}
  {% set value = config.get(key) %}
  {% if value is none or value == '' %}
    {{ exceptions.raise_compiler_error("Missing required vars.iceberg_sync." ~ key) }}
  {% endif %}
  {{ return(value) }}
{% endmacro %}

{#
  Resolve a deployment value that may be overridden by a dedicated top-level
  dbt var named 'iceberg_sync_<key>'. dbt renders jinja only in top-level
  string vars, never inside the nested vars.iceberg_sync map, so per-target
  values (for example a per-environment WIF audience) must come from a
  top-level var. Precedence: top-level var if set and non-empty, else the
  nested vars.iceberg_sync entry, else the default.
#}
{% macro iceberg_sync_deployment_var(deployment, key, default=none) %}
  {% set top_level = var('iceberg_sync_' ~ key, none) %}
  {% if top_level is not none and top_level != '' %}
    {{ return(top_level) }}
  {% endif %}
  {% set nested = deployment.get(key) %}
  {% if nested is not none and nested != '' %}
    {{ return(nested) }}
  {% endif %}
  {{ return(default) }}
{% endmacro %}

{% macro iceberg_sync_required_deployment_var(deployment, key) %}
  {% set value = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, key) %}
  {% if value is none or value == '' %}
    {{ exceptions.raise_compiler_error(
      "Missing required vars.iceberg_sync." ~ key ~ " (or top-level var iceberg_sync_" ~ key ~ ")."
    ) }}
  {% endif %}
  {{ return(value) }}
{% endmacro %}
