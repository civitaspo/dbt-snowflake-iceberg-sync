from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment


def test_json_sql_literal_round_trips_single_quotes_for_snowflake_sql():
    payload = {
        "incremental_predicate": "event_date = '20240111'",
        "model_sql": "select\n  *\nfrom `project.dataset.table`",
        "quoted_incremental_predicate": "\"event_date\" = '20240111'",
    }

    literal = _render_json_sql_literal(payload)

    assert "\\u0027" not in literal
    assert "\\\\n" in literal
    assert _decode_sql_string_literal(literal) == payload


def test_create_view_macro_quotes_uppercase_aliases():
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/relations.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_create_view_sql(target, internal, view_columns) }}"
    )

    rendered = template.render(
        {
            "target": '"DB"."SCHEMA"."VIEW"',
            "internal": '"DB"."SCHEMA"."TABLE"',
            "view_columns": [{"source_name": "OrderID", "alias": "select"}],
            "adapter": _FakeAdapter(),
        }
    )

    assert '"OrderID" AS "SELECT"' in rendered


def test_identifier_macros_normalize_snowflake_object_identifiers():
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/identifiers.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_normalize_object_identifier(value) }}"
    )

    rendered = template.render(
        {
            "value": ' "orders" ',
            "return": lambda item: item,
        }
    )

    assert rendered.strip() == "ORDERS"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (' "orders" ', '"ORDERS"'),
        ('my"stage', '"MY""STAGE"'),
        ('"my""stage"', '"MY""STAGE"'),
    ],
)
def test_quote_object_identifier_macro_escapes_embedded_quotes(value: str, expected: str):
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/identifiers.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_quote_object_identifier(value) }}"
    )

    rendered = template.render(
        {
            "value": value,
            "return": lambda item: item,
        }
    )

    assert rendered.strip() == expected


def test_internal_identifier_macro_normalizes_to_unquoted_snowflake_form():
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/identifiers.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_internal_identifier(target_relation) }}"
    )

    rendered = template.render(
        {
            "target_relation": SimpleNamespace(identifier="orders"),
            "dbt_snowflake_iceberg_sync": SimpleNamespace(
                iceberg_sync_normalize_object_identifier=_normalize_object_identifier
            ),
            "return": lambda item: item,
        }
    )

    assert rendered.strip() == "__ORDERS"


@pytest.mark.parametrize(
    ("target_database", "target_schema"),
    [
        ("analytics", "dbt_prod"),
        ("clone_analytics", "dbt_clone"),
        ("ci_analytics", "dbt_ci"),
    ],
)
def test_deployment_config_defaults_to_active_target_database_and_schema(
    target_database: str,
    target_schema: str,
):
    config = _render_deployment_config(
        _minimal_deployment_vars(),
        target_database=target_database,
        target_schema=target_schema,
    )

    expected_database = target_database.upper()
    expected_schema = target_schema.upper()

    assert config["procedure_database"] == expected_database
    assert config["procedure_schema"] == expected_schema
    assert config["procedure_name"] == "ICEBERG_SYNC"
    assert config["procedure_relation"] == {
        "database": expected_database,
        "schema": expected_schema,
        "identifier": "ICEBERG_SYNC",
    }
    assert (
        config["handler_stage"]
        == f'"{expected_database}"."{expected_schema}"."ICEBERG_SYNC_HANDLER_STAGE"'
    )
    assert config["run_log_table"] == {
        "database": expected_database,
        "schema": expected_schema,
        "identifier": "ICEBERG_SYNC_RUN_LOG",
    }


def test_procedure_fqn_renders_string_without_relation_type():
    rendered = _render_procedure_fqn(
        _minimal_deployment_vars(),
        target_database="clone_dbt",
        target_schema="dbt_clone",
    )

    assert rendered == '"CLONE_DBT"."DBT_CLONE"."ICEBERG_SYNC"'


def test_install_macro_uses_create_or_alter_procedure():
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/install.sql"
    macro_source = macro_path.read_text(encoding="utf-8")

    assert "CREATE OR ALTER PROCEDURE" in macro_source
    assert "CREATE OR REPLACE PROCEDURE" not in macro_source


def test_install_macro_uses_create_or_alter_for_run_log_table():
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/install.sql"
    macro_source = macro_path.read_text(encoding="utf-8")

    assert "CREATE OR ALTER TABLE" in macro_source
    assert "CREATE TABLE IF NOT EXISTS" not in macro_source
    assert "ADD COLUMN IF NOT EXISTS" not in macro_source


def test_type_normalization_accepts_snowflake_structured_type_canonicalization():
    existing = (
        "ARRAY(OBJECT(KEY VARCHAR(134217728), "
        "VALUE OBJECT(STRING_VALUE VARCHAR(134217728), INT_VALUE NUMBER(19,0), "
        "FLOAT_VALUE FLOAT, DOUBLE_VALUE FLOAT)))"
    )
    desired = (
        'ARRAY(OBJECT("key" VARCHAR, '
        '"value" OBJECT("string_value" VARCHAR, "int_value" BIGINT, '
        '"float_value" DOUBLE, "double_value" DOUBLE)))'
    )

    assert _render_normalized_snowflake_type(existing) == _render_normalized_snowflake_type(desired)


def test_deployment_config_honors_explicit_procedure_overrides():
    config = _render_deployment_config(
        {
            **_minimal_deployment_vars(),
            "procedure_database": "custom_analytics",
            "procedure_schema": "custom_deps",
            "procedure_name": "custom_iceberg_sync",
        },
        target_database="analytics",
        target_schema="dbt_user",
    )

    assert config["procedure_relation"] == {
        "database": "CUSTOM_ANALYTICS",
        "schema": "CUSTOM_DEPS",
        "identifier": "CUSTOM_ICEBERG_SYNC",
    }
    assert config["handler_stage"] == (
        '"CUSTOM_ANALYTICS"."CUSTOM_DEPS"."ICEBERG_SYNC_HANDLER_STAGE"'
    )
    assert config["run_log_table"] == {
        "database": "CUSTOM_ANALYTICS",
        "schema": "CUSTOM_DEPS",
        "identifier": "ICEBERG_SYNC_RUN_LOG",
    }


def test_deployment_config_honors_explicit_stage_and_run_log_table():
    config = _render_deployment_config(
        {
            **_minimal_deployment_vars(),
            "handler_stage": '"custom_analytics"."custom_deps"."custom_stage"',
            "run_log_table": "custom_analytics.custom_deps.custom_run_log",
        }
    )

    assert config["handler_stage"] == '"CUSTOM_ANALYTICS"."CUSTOM_DEPS"."CUSTOM_STAGE"'
    assert config["run_log_table"] == {
        "database": "CUSTOM_ANALYTICS",
        "schema": "CUSTOM_DEPS",
        "identifier": "CUSTOM_RUN_LOG",
    }


@pytest.mark.parametrize(
    "handler_stage",
    [
        'db.schema.my"stage',
        '"db"."schema"."my""stage"',
    ],
)
def test_deployment_config_escapes_quotes_in_stage_identifiers(handler_stage: str):
    config = _render_deployment_config(
        {
            **_minimal_deployment_vars(),
            "handler_stage": handler_stage,
        }
    )

    assert config["handler_stage"] == '"DB"."SCHEMA"."MY""STAGE"'


def test_deployment_config_quotes_secret_fqdn():
    config = _render_deployment_config(
        {
            **_minimal_deployment_vars(),
            "google_cloud_service_account_secret_fqdn": 'system.secrets."gcp""json"',
        }
    )

    assert config["google_cloud_service_account_secret_fqdn"] == '"SYSTEM"."SECRETS"."GCP""JSON"'


def test_deployment_config_accepts_workload_identity_federation_vars():
    vars_dict = _minimal_deployment_vars()
    vars_dict.pop("google_cloud_service_account_secret_fqdn")
    vars_dict["google_cloud_auth_method"] = "workload_identity_federation"
    vars_dict["google_cloud_workload_identity_federation_secret_fqdn"] = (
        "analytics.auth.workload_identity_federation_secret"
    )
    vars_dict["google_cloud_workload_identity_federation_audience"] = (
        "//iam.googleapis.com/projects/000000000000/locations/global/"
        "workloadIdentityPools/example-pool/providers/example-provider"
    )
    vars_dict["google_cloud_service_account_impersonation"] = (
        "sync@example-project.iam.gserviceaccount.com"
    )

    config = _render_deployment_config(vars_dict)

    assert config["google_cloud_auth_method"] == "workload_identity_federation"
    assert config["google_cloud_service_account_secret_fqdn"] is None
    assert config["google_cloud_workload_identity_federation_secret_fqdn"] == (
        "ANALYTICS.AUTH.WORKLOAD_IDENTITY_FEDERATION_SECRET"
    )
    assert config["google_cloud_workload_identity_federation_audience"].startswith(
        "//iam.googleapis.com/"
    )
    assert (
        config["google_cloud_service_account_impersonation"]
        == "sync@example-project.iam.gserviceaccount.com"
    )


def test_deployment_config_honors_top_level_workload_identity_federation_overrides():
    vars_dict = _minimal_deployment_vars()
    top_level_vars = {
        "iceberg_sync_google_cloud_auth_method": "workload_identity_federation",
        "iceberg_sync_google_cloud_workload_identity_federation_secret_fqdn": (
            "analytics.auth.top_level_secret"
        ),
        "iceberg_sync_google_cloud_workload_identity_federation_audience": (
            "//iam.googleapis.com/projects/000000000000/locations/global/"
            "workloadIdentityPools/example-pool/providers/example-provider"
        ),
    }

    config = _render_deployment_config(vars_dict, top_level_vars=top_level_vars)

    assert config["google_cloud_auth_method"] == "workload_identity_federation"
    assert (
        config["google_cloud_workload_identity_federation_secret_fqdn"]
        == "ANALYTICS.AUTH.TOP_LEVEL_SECRET"
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("bigquery_api", ['"BIGQUERY_API"']),
        (["bigquery_api", '"other_api"'], ['"BIGQUERY_API"', '"OTHER_API"']),
        (None, []),
    ],
)
def test_deployment_config_normalizes_external_access_integrations(
    value: object,
    expected: list[str],
):
    vars_dict = _minimal_deployment_vars()
    if value is not None:
        vars_dict["external_access_integrations"] = value

    config = _render_deployment_config(vars_dict)

    assert config["external_access_integrations"] == expected


@pytest.mark.parametrize(
    "missing_key",
    ["handler_local_path", "google_cloud_service_account_secret_fqdn"],
)
def test_deployment_config_keeps_credential_and_handler_path_vars_required(missing_key: str):
    vars_dict = _minimal_deployment_vars()
    vars_dict.pop(missing_key)

    with pytest.raises(RuntimeError, match=f"vars.iceberg_sync.{missing_key} is required"):
        _render_deployment_config(vars_dict)


class _FakeAdapter:
    def quote(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'


def _minimal_deployment_vars() -> dict[str, object]:
    return {
        "handler_local_path": "dbt_packages/dbt_snowflake_iceberg_sync/procedure",
        "google_cloud_service_account_secret_fqdn": (
            "SYSTEM.SECRETS.GOOGLE_CLOUD_SERVICE_ACCOUNT_JSON"
        ),
    }


def _render_deployment_config(
    vars_dict: dict[str, object],
    *,
    target_database: str = "analytics",
    target_schema: str = "dbt_user",
    top_level_vars: dict[str, object] | None = None,
) -> dict[str, object]:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/config.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8") + "\n{{ iceberg_sync_deployment_config() }}"
    )

    top_level_vars = top_level_vars or {}
    rendered = template.render(
        {
            "var": lambda name, default=None: (
                vars_dict if name == "iceberg_sync" else top_level_vars.get(name, default)
            ),
            "target": SimpleNamespace(database=target_database, schema=target_schema),
            "dbt_snowflake_iceberg_sync": SimpleNamespace(
                iceberg_sync_as_list=_as_list,
                iceberg_sync_defaulted_var=_defaulted_var,
                iceberg_sync_deployment_var=lambda current_vars, key, default=None: _deployment_var(
                    top_level_vars,
                    current_vars,
                    key,
                    default,
                ),
                iceberg_sync_normalize_object_identifier=_normalize_object_identifier,
                iceberg_sync_object_fqn=_object_fqn,
                iceberg_sync_quote_object_identifier=_quote_object_identifier,
                iceberg_sync_relation_from_fqn=_relation_from_fqn,
                iceberg_sync_required_var=_required_var,
            ),
            "return": lambda item: json.dumps(item, sort_keys=True),
        }
    )
    return json.loads(rendered.strip())


def _render_procedure_fqn(
    vars_dict: dict[str, object],
    *,
    target_database: str = "analytics",
    target_schema: str = "dbt_user",
) -> str:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/config.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8") + "\n{{ iceberg_sync_procedure_fqn() }}"
    )

    rendered = template.render(
        {
            "dbt_snowflake_iceberg_sync": SimpleNamespace(
                iceberg_sync_deployment_config=lambda: _render_deployment_config(
                    vars_dict,
                    target_database=target_database,
                    target_schema=target_schema,
                ),
                iceberg_sync_quote_object_identifier=_quote_object_identifier,
            ),
            "return": lambda item: item,
        }
    )
    return rendered.strip()


def _render_normalized_snowflake_type(value: str) -> str:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/orchestration.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_normalized_snowflake_type(value) }}"
    )

    return template.render(
        {
            "modules": SimpleNamespace(re=re),
            "return": lambda item: item,
            "value": value,
        }
    ).strip()


def _defaulted_var(vars_dict: dict[str, object], key: str, default: object) -> object:
    value = vars_dict.get(key)
    if value is None or value == "":
        return default
    return value


def _deployment_var(
    top_level_vars: dict[str, object],
    vars_dict: dict[str, object],
    key: str,
    default: object,
) -> object:
    return top_level_vars.get(f"iceberg_sync_{key}", _defaulted_var(vars_dict, key, default))


def _required_var(vars_dict: dict[str, object], key: str) -> object:
    value = vars_dict.get(key)
    if value is None or value == "":
        raise RuntimeError(f"vars.iceberg_sync.{key} is required")
    return value


def _as_list(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)  # type: ignore[arg-type]


def _normalize_object_identifier(value: object) -> str:
    return str(value).strip().replace('"', "").upper()


def _quote_object_identifier(value: object) -> str:
    identifier = str(value).strip()
    if len(identifier) >= 2 and identifier[0] == '"' and identifier[-1] == '"':
        identifier = identifier[1:-1].replace('""', '"')
    return '"' + identifier.upper().replace('"', '""') + '"'


def _object_fqn(
    value: object,
    field_name: str,
    min_parts: int = 1,
    max_parts: int = 3,
) -> str:
    parts = str(value).split(".")
    if len(parts) < min_parts or len(parts) > max_parts:
        raise RuntimeError(f"{field_name} must have between {min_parts} and {max_parts} parts")
    if any(not str(part).strip() for part in parts):
        raise RuntimeError(f"{field_name} contains an empty identifier")
    return ".".join(_quote_object_identifier(part) for part in parts)


def _relation_from_fqn(value: object, field_name: str) -> dict[str, str]:
    parts = str(value).split(".")
    if len(parts) != 3:
        raise RuntimeError(f"{field_name} must be a three-part relation name")
    return {
        "database": _normalize_object_identifier(parts[0]),
        "schema": _normalize_object_identifier(parts[1]),
        "identifier": _normalize_object_identifier(parts[2]),
    }


def _render_json_sql_literal(value: dict[str, str]) -> str:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/json.sql"
    template = Environment().from_string(
        macro_path.read_text(encoding="utf-8") + "\n{{ iceberg_sync_json_sql_literal(value) }}"
    )
    return template.render({"value": value, "return": lambda item: item}).strip()


def _decode_sql_string_literal(literal: str) -> dict[str, str]:
    assert literal.startswith("'") and literal.endswith("'")
    json_text = literal[1:-1].replace("''", "'").replace("\\\\", "\\")
    return json.loads(json_text)
