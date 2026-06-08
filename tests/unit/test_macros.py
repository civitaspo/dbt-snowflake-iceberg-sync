from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment


def test_json_sql_literal_round_trips_single_quotes_for_snowflake_sql():
    payload = {
        "incremental_predicate": "event_date = '20240111'",
        "model_sql": "select\n  *\nfrom `project.dataset.table`",
        "quoted_incremental_predicate": '"event_date" = \'20240111\'',
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
) -> dict[str, object]:
    macro_path = Path(__file__).resolve().parents[2] / "macros/iceberg_sync/config.sql"
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        macro_path.read_text(encoding="utf-8") + "\n{{ iceberg_sync_deployment_config() }}"
    )

    rendered = template.render(
        {
            "var": lambda name, default=None: vars_dict if name == "iceberg_sync" else default,
            "target": SimpleNamespace(database=target_database, schema=target_schema),
            "dbt_snowflake_iceberg_sync": SimpleNamespace(
                iceberg_sync_defaulted_var=_defaulted_var,
                iceberg_sync_normalize_object_identifier=_normalize_object_identifier,
                iceberg_sync_object_fqn=_object_fqn,
                iceberg_sync_relation_from_fqn=_relation_from_fqn,
                iceberg_sync_required_var=_required_var,
            ),
            "return": lambda item: json.dumps(item, sort_keys=True),
        }
    )
    return json.loads(rendered.strip())


def _defaulted_var(vars_dict: dict[str, object], key: str, default: object) -> object:
    value = vars_dict.get(key)
    if value is None or value == "":
        return default
    return value


def _required_var(vars_dict: dict[str, object], key: str) -> object:
    value = vars_dict.get(key)
    if value is None or value == "":
        raise RuntimeError(f"vars.iceberg_sync.{key} is required")
    return value


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
        raise RuntimeError(
            f"{field_name} must have between {min_parts} and {max_parts} parts"
        )
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
        macro_path.read_text(encoding="utf-8")
        + "\n{{ iceberg_sync_json_sql_literal(value) }}"
    )
    return template.render({"value": value, "return": lambda item: item}).strip()


def _decode_sql_string_literal(literal: str) -> dict[str, str]:
    assert literal.startswith("'") and literal.endswith("'")
    json_text = literal[1:-1].replace("''", "'").replace("\\\\", "\\")
    return json.loads(json_text)
