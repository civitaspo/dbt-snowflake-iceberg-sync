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


def test_deployment_config_resolves_workload_identity_federation_by_dbt_target():
    vars_dict = {
        "handler_local_path": "dbt_packages/dbt_snowflake_iceberg_sync/procedure",
        "google_cloud_auth_method": "workload_identity_federation",
        "google_cloud_workload_identity_federation_by_dbt_target": {
            "dev-via-sso": {
                "google_cloud_workload_identity_federation_secret_fqdn": (
                    "system.secrets.workload_identity_federation_default"
                ),
                "google_cloud_workload_identity_federation_audience": (
                    "//iam.googleapis.com/projects/966890289127/locations/global/"
                    "workloadIdentityPools/snowflake-oidc/providers/snowflake-provider"
                ),
                "google_cloud_service_account_impersonation": (
                    "snowflake@l1-snowflake-dev.iam.gserviceaccount.com"
                ),
            },
            "stg-via-sso": {
                "google_cloud_workload_identity_federation_secret_fqdn": (
                    "system.secrets.workload_identity_federation_default"
                ),
                "google_cloud_workload_identity_federation_audience": (
                    "//iam.googleapis.com/projects/1050948979201/locations/global/"
                    "workloadIdentityPools/snowflake-oidc/providers/snowflake-provider"
                ),
                "google_cloud_service_account_impersonation": (
                    "snowflake@l1-snowflake-stg.iam.gserviceaccount.com"
                ),
            },
        },
    }

    dev_config = _render_deployment_config(
        vars_dict,
        target_name="dev-via-sso",
    )
    stg_config = _render_deployment_config(
        vars_dict,
        target_name="stg-via-sso",
    )

    assert "966890289127" in dev_config["google_cloud_workload_identity_federation_audience"]
    assert dev_config["google_cloud_service_account_impersonation"].endswith(
        "l1-snowflake-dev.iam.gserviceaccount.com"
    )
    assert "1050948979201" in stg_config["google_cloud_workload_identity_federation_audience"]
    assert stg_config["google_cloud_service_account_impersonation"].endswith(
        "l1-snowflake-stg.iam.gserviceaccount.com"
    )


def test_deployment_config_prefers_by_dbt_target_over_flat_workload_identity_federation_vars():
    vars_dict = {
        "handler_local_path": "dbt_packages/dbt_snowflake_iceberg_sync/procedure",
        "google_cloud_auth_method": "workload_identity_federation",
        "google_cloud_workload_identity_federation_secret_fqdn": (
            "analytics.auth.flat_secret"
        ),
        "google_cloud_workload_identity_federation_audience": (
            "//iam.googleapis.com/projects/flat/locations/global/"
            "workloadIdentityPools/flat/providers/flat"
        ),
        "google_cloud_service_account_impersonation": (
            "flat@example-project.iam.gserviceaccount.com"
        ),
        "google_cloud_workload_identity_federation_by_dbt_target": {
            "dev-via-sso": {
                "google_cloud_workload_identity_federation_secret_fqdn": (
                    "analytics.auth.target_secret"
                ),
                "google_cloud_workload_identity_federation_audience": (
                    "//iam.googleapis.com/projects/target/locations/global/"
                    "workloadIdentityPools/target/providers/target"
                ),
                "google_cloud_service_account_impersonation": (
                    "target@example-project.iam.gserviceaccount.com"
                ),
            }
        },
    }

    config = _render_deployment_config(vars_dict, target_name="dev-via-sso")

    assert (
        config["google_cloud_workload_identity_federation_secret_fqdn"]
        == "ANALYTICS.AUTH.TARGET_SECRET"
    )
    assert "/projects/target/" in config["google_cloud_workload_identity_federation_audience"]
    assert (
        config["google_cloud_service_account_impersonation"]
        == "target@example-project.iam.gserviceaccount.com"
    )


def test_deployment_config_falls_back_to_default_by_dbt_target_entry():
    vars_dict = {
        "handler_local_path": "dbt_packages/dbt_snowflake_iceberg_sync/procedure",
        "google_cloud_auth_method": "workload_identity_federation",
        "google_cloud_workload_identity_federation_by_dbt_target": {
            "default": {
                "google_cloud_workload_identity_federation_secret_fqdn": (
                    "analytics.auth.default_secret"
                ),
                "google_cloud_workload_identity_federation_audience": (
                    "//iam.googleapis.com/projects/default/locations/global/"
                    "workloadIdentityPools/default/providers/default"
                ),
                "google_cloud_service_account_impersonation": (
                    "default@example-project.iam.gserviceaccount.com"
                ),
            }
        },
    }

    config = _render_deployment_config(vars_dict, target_name="unknown-target")

    assert (
        config["google_cloud_workload_identity_federation_secret_fqdn"]
        == "ANALYTICS.AUTH.DEFAULT_SECRET"
    )
    assert "/projects/default/" in config["google_cloud_workload_identity_federation_audience"]


def test_deployment_config_falls_back_to_flat_when_by_dbt_target_has_no_match():
    vars_dict = {
        "handler_local_path": "dbt_packages/dbt_snowflake_iceberg_sync/procedure",
        "google_cloud_auth_method": "workload_identity_federation",
        "google_cloud_workload_identity_federation_secret_fqdn": (
            "analytics.auth.flat_secret"
        ),
        "google_cloud_workload_identity_federation_audience": (
            "//iam.googleapis.com/projects/flat/locations/global/"
            "workloadIdentityPools/flat/providers/flat"
        ),
        "google_cloud_service_account_impersonation": (
            "flat@example-project.iam.gserviceaccount.com"
        ),
        "google_cloud_workload_identity_federation_by_dbt_target": {
            "dev-via-sso": {
                "google_cloud_workload_identity_federation_secret_fqdn": (
                    "analytics.auth.target_secret"
                ),
                "google_cloud_workload_identity_federation_audience": (
                    "//iam.googleapis.com/projects/target/locations/global/"
                    "workloadIdentityPools/target/providers/target"
                ),
                "google_cloud_service_account_impersonation": (
                    "target@example-project.iam.gserviceaccount.com"
                ),
            }
        },
    }

    config = _render_deployment_config(vars_dict, target_name="unknown-target")

    assert (
        config["google_cloud_workload_identity_federation_secret_fqdn"]
        == "ANALYTICS.AUTH.FLAT_SECRET"
    )
    assert "/projects/flat/" in config["google_cloud_workload_identity_federation_audience"]


def test_deployment_config_raises_when_workload_identity_federation_settings_missing():
    vars_dict = {
        "handler_local_path": "dbt_packages/dbt_snowflake_iceberg_sync/procedure",
        "google_cloud_auth_method": "workload_identity_federation",
        "google_cloud_workload_identity_federation_by_dbt_target": {
            "dev-via-sso": {
                "google_cloud_workload_identity_federation_audience": (
                    "//iam.googleapis.com/projects/966890289127/locations/global/"
                    "workloadIdentityPools/snowflake-oidc/providers/snowflake-provider"
                ),
            }
        },
    }

    with pytest.raises(RuntimeError) as exc_info:
        _render_deployment_config(vars_dict, target_name="dev-via-sso")

    message = str(exc_info.value)
    assert "google_cloud_workload_identity_federation_secret_fqdn" in message
    assert "Available by_dbt_target keys: 'dev-via-sso'" in message


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


def test_model_config_reads_meta_iceberg_sync_only():
    value = _render_model_config(
        config_name="google_cloud_project_id",
        default=None,
        top_level={},
        meta_iceberg_sync={"google_cloud_project_id": "meta-project"},
    )

    assert value == "meta-project"


def test_model_config_falls_back_to_legacy_top_level():
    value = _render_model_config(
        config_name="google_cloud_project_id",
        default=None,
        top_level={"google_cloud_project_id": "top-level-project"},
        meta_iceberg_sync={},
    )

    assert value == "top-level-project"


def test_model_config_prefers_meta_iceberg_sync_over_top_level():
    value = _render_model_config(
        config_name="google_cloud_project_id",
        default=None,
        top_level={"google_cloud_project_id": "top-level-project"},
        meta_iceberg_sync={"google_cloud_project_id": "meta-project"},
    )

    assert value == "meta-project"


@pytest.mark.parametrize(
    ("config_name", "meta_value", "expected"),
    [
        ("partition_by", ["event_date"], ["event_date"]),
        ("cluster_by", "event_name", ["event_name"]),
        ("partition_by", [], []),
        ("cluster_by", None, []),
    ],
)
def test_model_config_reads_partition_and_cluster_from_meta(
    config_name: str,
    meta_value: object,
    expected: list[object],
):
    meta_iceberg_sync = {}
    if meta_value is not None:
        meta_iceberg_sync[config_name] = meta_value

    value = _render_model_config_as_list(
        config_name=config_name,
        top_level={},
        meta_iceberg_sync=meta_iceberg_sync,
    )

    assert value == expected


@pytest.mark.parametrize(
    "source",
    ["top_level", "meta"],
)
def test_forbidden_credential_keys_rejected_from_top_level_and_meta(source: str):
    top_level: dict[str, object] = {}
    meta_iceberg_sync: dict[str, object] = {}
    if source == "top_level":
        top_level["google_cloud_service_account_secret_fqdn"] = "secret.fqdn"
    else:
        meta_iceberg_sync["google_cloud_service_account_secret_fqdn"] = "secret.fqdn"

    with pytest.raises(
        RuntimeError,
        match="credential material must not be set in model config: "
        "google_cloud_service_account_secret_fqdn",
    ):
        _render_forbidden_model_config_validation(
            top_level=top_level,
            meta_iceberg_sync=meta_iceberg_sync,
        )


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


class _MacroReturn(Exception):
    def __init__(self, value: object):
        self.value = value


def _macro_return(value: object) -> None:
    raise _MacroReturn(value)


def _model_node_with_meta(meta_iceberg_sync: dict[str, object]) -> SimpleNamespace:
    meta: dict[str, object] = {}
    if meta_iceberg_sync:
        meta["iceberg_sync"] = meta_iceberg_sync
    return SimpleNamespace(config=SimpleNamespace(meta=meta))


class _ConfigGet:
    def __init__(self, values: dict[str, object]):
        self._values = values

    def get(self, key: str, default: object = None) -> object:
        return self._values.get(key, default)


def _render_model_config(
    *,
    config_name: str,
    default: object,
    top_level: dict[str, object],
    meta_iceberg_sync: dict[str, object],
) -> object:
    validation_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/validation.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        validation_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_model_config(model_node, config_name, default) }}"
    )
    model_node = _model_node_with_meta(meta_iceberg_sync)
    package = SimpleNamespace(
        iceberg_sync_model_meta=lambda node: (
            node.config.meta.get("iceberg_sync", {})
            if getattr(node.config, "meta", None)
            else {}
        )
    )
    try:
        template.render(
            {
                "model_node": model_node,
                "config_name": config_name,
                "default": default,
                "config": _ConfigGet(top_level),
                "dbt_snowflake_iceberg_sync": package,
                "return": _macro_return,
            }
        )
    except _MacroReturn as returned:
        return returned.value
    raise AssertionError("iceberg_sync_model_config did not return a value")


def _render_model_config_as_list(
    *,
    config_name: str,
    top_level: dict[str, object],
    meta_iceberg_sync: dict[str, object],
) -> list[object]:
    value = _render_model_config(
        config_name=config_name,
        default=[],
        top_level=top_level,
        meta_iceberg_sync=meta_iceberg_sync,
    )
    return _as_list(value)


def _render_forbidden_model_config_validation(
    *,
    top_level: dict[str, object],
    meta_iceberg_sync: dict[str, object],
) -> None:
    validation_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/validation.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        validation_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_validate_forbidden_model_configs(model_node) }}"
    )
    model_node = _model_node_with_meta(meta_iceberg_sync)
    package = SimpleNamespace(
        iceberg_sync_model_meta=lambda node: (
            node.config.meta.get("iceberg_sync", {})
            if getattr(node.config, "meta", None)
            else {}
        ),
        iceberg_sync_raise=_raise,
    )
    template.render(
        {
            "model_node": model_node,
            "config": _ConfigGet(top_level),
            "dbt_snowflake_iceberg_sync": package,
            "return": _macro_return,
        }
    )


def _render_deployment_config(
    vars_dict: dict[str, object],
    *,
    target_database: str = "analytics",
    target_schema: str = "dbt_user",
    target_name: str = "dev",
    top_level_vars: dict[str, object] | None = None,
) -> dict[str, object]:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/config.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8") + "\n{{ iceberg_sync_deployment_config() }}"
    )

    top_level_vars = top_level_vars or {}
    package_namespace = SimpleNamespace(
        iceberg_sync_as_list=_as_list,
        iceberg_sync_defaulted_var=_defaulted_var,
        iceberg_sync_deployment_var=lambda current_vars, key, default=None: _deployment_var(
            top_level_vars,
            current_vars,
            key,
            default,
        ),
        iceberg_sync_workload_identity_federation_by_dbt_target_entry_var=(
            lambda entry_settings, entry_label, field_key: (
                _workload_identity_federation_by_dbt_target_entry_var(
                    entry_settings,
                    entry_label,
                    field_key,
                )
            )
        ),
        iceberg_sync_workload_identity_federation_config_hint=(
            lambda current_vars, key: _workload_identity_federation_config_hint(
                current_vars,
                key,
                target_name=target_name,
            )
        ),
        iceberg_sync_workload_identity_federation_deployment_var=(
            lambda current_vars, key, default=None: _workload_identity_federation_deployment_var(
                top_level_vars,
                current_vars,
                key,
                default,
                target_name=target_name,
            )
        ),
        iceberg_sync_normalize_object_identifier=_normalize_object_identifier,
        iceberg_sync_object_fqn=_object_fqn,
        iceberg_sync_quote_object_identifier=_quote_object_identifier,
        iceberg_sync_relation_from_fqn=_relation_from_fqn,
        iceberg_sync_required_var=_required_var,
        iceberg_sync_raise=_raise,
    )
    rendered = template.render(
        {
            "var": lambda name, default=None: (
                vars_dict if name == "iceberg_sync" else top_level_vars.get(name, default)
            ),
            "target": SimpleNamespace(
                database=target_database,
                schema=target_schema,
                name=target_name,
            ),
            "dbt_snowflake_iceberg_sync": package_namespace,
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


def _workload_identity_federation_by_dbt_target_entry_var(
    entry_settings: object,
    entry_label: str,
    key: str,
) -> object:
    if entry_settings is None:
        return None
    if not isinstance(entry_settings, dict):
        raise RuntimeError(
            "iceberg_sync: "
            "vars.iceberg_sync.google_cloud_workload_identity_federation_by_dbt_target"
            f"['{entry_label}'] must be a mapping"
        )
    entry_value = entry_settings.get(key)
    if entry_value is not None and entry_value != "":
        return entry_value
    return None


def _workload_identity_federation_deployment_var(
    top_level_vars: dict[str, object],
    vars_dict: dict[str, object],
    key: str,
    default: object,
    *,
    target_name: str,
) -> object:
    override = top_level_vars.get(f"iceberg_sync_{key}")
    if override is not None and override != "":
        return override

    by_dbt_target = vars_dict.get("google_cloud_workload_identity_federation_by_dbt_target")
    if by_dbt_target is not None and not isinstance(by_dbt_target, dict):
        raise RuntimeError(
            "iceberg_sync: "
            "vars.iceberg_sync.google_cloud_workload_identity_federation_by_dbt_target "
            "must be a mapping"
        )
    if isinstance(by_dbt_target, dict):
        for entry_label, entry_settings in (
            (target_name, by_dbt_target.get(target_name)),
            ("default", by_dbt_target.get("default")),
        ):
            entry_value = _workload_identity_federation_by_dbt_target_entry_var(
                entry_settings,
                entry_label,
                key,
            )
            if entry_value is not None:
                return entry_value

    return _defaulted_var(vars_dict, key, default)


def _workload_identity_federation_config_hint(
    vars_dict: dict[str, object],
    key: str,
    *,
    target_name: str,
) -> str:
    by_dbt_target = vars_dict.get("google_cloud_workload_identity_federation_by_dbt_target", {})
    map_keys = []
    if isinstance(by_dbt_target, dict):
        map_keys = sorted(f"'{map_key}'" for map_key in by_dbt_target)
    map_keys_text = ", ".join(map_keys) if map_keys else "(none)"
    has_default = isinstance(by_dbt_target, dict) and by_dbt_target.get("default") is not None
    default_hint = " or ['default']" if has_default else " (no 'default' entry)"
    return (
        f"Configure vars.iceberg_sync.{key} (or top-level var iceberg_sync_{key}), "
        f"vars.iceberg_sync.google_cloud_workload_identity_federation_by_dbt_target['{target_name}']"
        f"{default_hint}. Available by_dbt_target keys: {map_keys_text} "
        "when google_cloud_auth_method='workload_identity_federation'"
    )


def _raise(message: str) -> None:
    raise RuntimeError(f"iceberg_sync: {message}")


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
