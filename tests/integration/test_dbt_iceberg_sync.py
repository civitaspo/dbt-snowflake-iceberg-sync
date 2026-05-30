from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class IntegrationContext:
    run_id: str
    project_dir: Path
    profiles_dir: Path
    package_path: Path
    snowflake_database: str
    snowflake_schema: str
    procedure_database: str
    procedure_schema: str
    procedure_name: str
    run_log_relation: str
    handler_stage: str
    handler_stage_from_env: str | None
    export_stage: str
    external_volume: str
    external_access_integration: str
    secret_fqdn: str
    secret_alias: str
    bigquery_project_id: str
    bigquery_dataset_id: str
    bigquery_location: str

    @property
    def procedure_relation(self) -> str:
        return f"{self.procedure_database}.{self.procedure_schema}.{self.procedure_name}"


def test_dbt_extract_smoke(tmp_path: Path):
    context = _integration_context(tmp_path, "smoke")
    model_name = f"iceberg_sync_smoke_{context.run_id}"
    export_prefix = _export_prefix(context, model_name)
    models = {
        model_name: _extract_model_sql(
            context,
            model_name=model_name,
            table_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID"),
            export_predicate_type="none",
            base_location=export_prefix,
            export_prefix=export_prefix,
        )
    }

    _write_project(context, models)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run", "--select", model_name)
        _assert_models(
            context,
            [
                _assertion(
                    context,
                    model_name,
                    expected_modes=["full_refresh"],
                    expected_rows=_optional_int_env(
                        "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_EXPECTED_ROWS"
                    ),
                )
            ],
        )
    finally:
        _cleanup(context, [model_name])


def test_dbt_extract_modes(tmp_path: Path):
    context = _integration_context(tmp_path, "extract_modes")
    cases = [
        {
            "suffix": "non_partitioned",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "time_partitioned",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [
                _required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_DECORATOR"
                )
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "range_partitioned",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [
                _required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_DECORATOR"
                )
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_EXPECTED_ROWS"
            ),
        },
    ]
    model_sql: dict[str, str] = {}
    assertions: list[dict[str, Any]] = []
    model_names: list[str] = []
    for case in cases:
        model_name = f"iceberg_sync_{case['suffix']}_{context.run_id}"
        export_prefix = _export_prefix(context, model_name)
        model_names.append(model_name)
        model_sql[model_name] = _extract_model_sql(
            context,
            model_name=model_name,
            table_id=str(case["table_id"]),
            export_predicate_type=str(case["export_predicate_type"]),
            full_refresh_predicates=case["full_refresh_predicates"],
            base_location=export_prefix,
            export_prefix=export_prefix,
        )
        assertions.append(
            _assertion(
                context,
                model_name,
                expected_rows=int(case["expected_rows"]),
                expected_modes=["full_refresh"],
            )
        )

    _write_project(context, model_sql)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run")
        _assert_models(context, assertions)
    finally:
        _cleanup(context, model_names)


def test_dbt_select_query_export(tmp_path: Path):
    context = _integration_context(tmp_path, "select")
    model_name = f"iceberg_sync_select_{context.run_id}"
    export_prefix = _export_prefix(context, model_name)
    model_sql = {
        model_name: _select_model_sql(
            context,
            model_name=model_name,
            model_sql=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_SQL"),
            predicate=_required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE"
            ),
            table_id=os.environ.get(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_TABLE_ID",
                "select_query_source",
            ),
            staging_dataset_id=_required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
            ),
            base_location=export_prefix,
            export_prefix=export_prefix,
        )
    }

    _write_project(context, model_sql)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run", "--select", model_name)
        _assert_models(
            context,
            [
                _assertion(
                    context,
                    model_name,
                    expected_rows=_required_int_env(
                        "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS"
                    ),
                    expected_modes=["full_refresh"],
                    require_staging_table=True,
                )
            ],
        )
    finally:
        _cleanup(context, [model_name])


def test_dbt_incremental_delete_copy(tmp_path: Path):
    context = _integration_context(tmp_path, "incremental")
    model_name = f"iceberg_sync_incremental_{context.run_id}"
    export_prefix = _export_prefix(context, model_name)
    model_sql = {
        model_name: _extract_model_sql(
            context,
            model_name=model_name,
            table_id=_required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_TABLE_ID"
            ),
            export_predicate_type=os.environ.get(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_EXPORT_PREDICATE_TYPE",
                "auto",
            ),
            materialization_strategy="incremental",
            full_refresh_predicates=_required_env_list(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_FULL_REFRESH_PREDICATES"
            ),
            incremental_predicates=_required_env_list(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_PREDICATES"
            ),
            incremental_predicate=_required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_PREDICATE"
            ),
            base_location=export_prefix,
            export_prefix=export_prefix,
        )
    }

    _write_project(context, model_sql)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run", "--select", model_name)
        _run_dbt(context, "run", "--select", model_name)
        _run_dbt(context, "run", "--select", model_name)
        _assert_models(
            context,
            [
                _assertion(
                    context,
                    model_name,
                    expected_rows=_required_int_env(
                        "DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_EXPECTED_ROWS"
                    ),
                    expected_modes=["full_refresh", "incremental", "incremental"],
                )
            ],
        )
    finally:
        _cleanup(context, [model_name])


def _integration_context(tmp_path: Path, prefix: str) -> IntegrationContext:
    if os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION") != "1":
        pytest.skip("set DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1 to run")

    run_id = uuid.uuid4().hex[:12]
    snowflake_database = _required_env("SNOWFLAKE_DATABASE")
    snowflake_schema = _required_env("SNOWFLAKE_SCHEMA")
    procedure_database = os.environ.get(
        "DBT_SNOWFLAKE_ICEBERG_SYNC_PROCEDURE_DATABASE",
        snowflake_database,
    )
    procedure_schema = os.environ.get(
        "DBT_SNOWFLAKE_ICEBERG_SYNC_PROCEDURE_SCHEMA",
        snowflake_schema,
    )
    handler_stage_from_env = os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_HANDLER_STAGE")
    handler_stage = handler_stage_from_env or (
        f"{procedure_database}.{procedure_schema}.ICEBERG_SYNC_HANDLER_STAGE_{run_id.upper()}"
    )
    return IntegrationContext(
        run_id=run_id,
        project_dir=tmp_path / f"dbt_project_{prefix}",
        profiles_dir=tmp_path / f"profiles_{prefix}",
        package_path=Path(
            os.environ.get(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_PACKAGE_PATH",
                Path(__file__).resolve().parents[2],
            )
        ),
        snowflake_database=snowflake_database,
        snowflake_schema=snowflake_schema,
        procedure_database=procedure_database,
        procedure_schema=procedure_schema,
        procedure_name=f"ICEBERG_SYNC_TEST_{prefix.upper()}_{run_id.upper()}",
        run_log_relation=(
            f"{procedure_database}.{procedure_schema}.ICEBERG_SYNC_RUN_LOG_{prefix.upper()}_"
            f"{run_id.upper()}"
        ),
        handler_stage=handler_stage,
        handler_stage_from_env=handler_stage_from_env,
        export_stage=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_EXPORT_STAGE"),
        external_volume=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_VOLUME"),
        external_access_integration=_required_env(
            "DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_ACCESS_INTEGRATION"
        ),
        secret_fqdn=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_FQDN"),
        secret_alias=os.environ.get(
            "DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_ALIAS",
            "google_cloud_service_account_credentials_json",
        ),
        bigquery_project_id=_required_env(
            "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID"
        ),
        bigquery_dataset_id=_required_env(
            "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID"
        ),
        bigquery_location=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_LOCATION"),
    )


def _write_project(context: IntegrationContext, models: dict[str, str]) -> None:
    (context.project_dir / "models").mkdir(parents=True)
    (context.project_dir / "macros").mkdir(parents=True)
    context.profiles_dir.mkdir(parents=True)

    (context.project_dir / "packages.yml").write_text(
        f"packages:\n  - local: {context.package_path}\n",
        encoding="utf-8",
    )
    (context.project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            f"""
            name: iceberg_sync_integration_{context.run_id}
            version: 1.0.0
            config-version: 2
            profile: iceberg_sync_integration
            model-paths: [models]
            macro-paths: [macros]
            on-run-start:
              - "{{{{ dbt_snowflake_iceberg_sync.install_iceberg_sync_procedure() }}}}"
            vars:
              iceberg_sync:
                procedure_database: {context.procedure_database}
                procedure_schema: {context.procedure_schema}
                procedure_name: {context.procedure_name}
                run_log_table: {context.run_log_relation}
                handler_stage: {context.handler_stage}
                handler_stage_path: procedure
                handler_import_name: iceberg_sync_procedure_{context.run_id}
                handler_name: iceberg_sync_procedure_{context.run_id}.handler.main
                handler_local_path: dbt_packages/dbt_snowflake_iceberg_sync/procedure
                external_access_integrations:
                  - {context.external_access_integration}
                google_cloud_service_account_secret_fqdn: {context.secret_fqdn}
                google_cloud_service_account_secret_alias: {context.secret_alias}
            """
        ).lstrip(),
        encoding="utf-8",
    )
    for model_name, sql in models.items():
        (context.project_dir / "models" / f"{model_name}.sql").write_text(
            sql,
            encoding="utf-8",
        )
    (context.project_dir / "macros" / "integration_assertions.sql").write_text(
        _assertion_macros(),
        encoding="utf-8",
    )
    (context.profiles_dir / "profiles.yml").write_text(
        _profile_yaml(
            database=context.snowflake_database,
            schema=context.snowflake_schema,
        ),
        encoding="utf-8",
    )


def _extract_model_sql(
    context: IntegrationContext,
    *,
    model_name: str,
    table_id: str,
    export_predicate_type: str,
    base_location: str,
    export_prefix: str,
    materialization_strategy: str = "full_refresh",
    full_refresh_predicates: list[str] | None = None,
    incremental_predicates: list[str] | None = None,
    incremental_predicate: str | None = None,
) -> str:
    incremental_config = ""
    incremental_setup = ""
    if incremental_predicate is not None:
        incremental_setup = textwrap.dedent(
            f"""
            {{% set iceberg_sync_incremental_predicate %}}
            {incremental_predicate}
            {{% endset %}}

            """
        )
        incremental_config = textwrap.dedent(
            """
                incremental_strategy='delete+copy',
                incremental_predicate=iceberg_sync_incremental_predicate | trim,
            """
        )
    return textwrap.dedent(
        f"""
        {incremental_setup}
        {{{{
          config(
            materialized='iceberg_sync',
            source_type='bigquery',
            materialization_strategy={_jstr(materialization_strategy)},
        {textwrap.indent(incremental_config, '    ').rstrip()}
            bigquery_export_strategy='extract',
            google_cloud_project_id={_jstr(context.bigquery_project_id)},
            bigquery_dataset_id={_jstr(context.bigquery_dataset_id)},
            bigquery_table_id={_jstr(table_id)},
            bigquery_location={_jstr(context.bigquery_location)},
            bigquery_export_location={_jstr('@' + context.export_stage + '/' + export_prefix)},
            bigquery_export_predicate_type={_jstr(export_predicate_type)},
            bigquery_export_full_refresh_predicates={_jlist(full_refresh_predicates or [])},
            bigquery_export_incremental_predicates={_jlist(incremental_predicates or [])},
            iceberg_table_external_volume={_jstr(context.external_volume)},
            iceberg_table_base_location={_jstr(base_location)}
          )
        }}}}
        """
    ).lstrip()


def _select_model_sql(
    context: IntegrationContext,
    *,
    model_name: str,
    model_sql: str,
    predicate: str,
    table_id: str,
    staging_dataset_id: str,
    base_location: str,
    export_prefix: str,
) -> str:
    return textwrap.dedent(
        f"""
        {{{{
          config(
            materialized='iceberg_sync',
            source_type='bigquery',
            materialization_strategy='full_refresh',
            bigquery_export_strategy='select',
            google_cloud_project_id={_jstr(context.bigquery_project_id)},
            bigquery_dataset_id={_jstr(context.bigquery_dataset_id)},
            bigquery_table_id={_jstr(table_id)},
            bigquery_location={_jstr(context.bigquery_location)},
            bigquery_export_location={_jstr('@' + context.export_stage + '/' + export_prefix)},
            bigquery_export_predicate_type='where',
            bigquery_export_full_refresh_predicates={_jlist([predicate])},
            bigquery_staging_dataset_id={_jstr(staging_dataset_id)},
            bigquery_staging_table_reuse=false,
            iceberg_table_external_volume={_jstr(context.external_volume)},
            iceberg_table_base_location={_jstr(base_location)}
          )
        }}}}

        {model_sql}
        """
    ).lstrip()


def _assertion(
    context: IntegrationContext,
    model_name: str,
    *,
    expected_rows: int | None,
    expected_modes: list[str],
    require_staging_table: bool = False,
) -> dict[str, Any]:
    return {
        "view_relation": _unquoted_relation(
            context.snowflake_database,
            context.snowflake_schema,
            model_name,
        ),
        "internal_relation": _quoted_relation(
            context.snowflake_database,
            context.snowflake_schema,
            f"__{model_name}",
        ),
        "target_view": _quoted_relation(
            context.snowflake_database,
            context.snowflake_schema,
            model_name,
        ),
        "expected_rows": expected_rows,
        "expected_modes": expected_modes,
        "require_staging_table": require_staging_table,
    }


def _assert_models(context: IntegrationContext, models: list[dict[str, Any]]) -> None:
    _run_dbt(
        context,
        "run-operation",
        "assert_iceberg_sync_integration",
        "--args",
        json.dumps({"models": models, "run_log_relation": context.run_log_relation}),
    )


def _cleanup(context: IntegrationContext, model_names: list[str]) -> None:
    objects = [
        {
            "view_relation": _unquoted_relation(
                context.snowflake_database,
                context.snowflake_schema,
                model_name,
            ),
            "internal_relation": _quoted_relation(
                context.snowflake_database,
                context.snowflake_schema,
                f"__{model_name}",
            ),
        }
        for model_name in model_names
    ]
    subprocess.run(
        [
            _dbt_executable(),
            "run-operation",
            "cleanup_iceberg_sync_integration",
            "--args",
            json.dumps(
                {
                    "objects": objects,
                    "procedure_relation": context.procedure_relation,
                    "run_log_relation": context.run_log_relation,
                    "handler_stage": context.handler_stage,
                    "drop_handler_stage": context.handler_stage_from_env is None,
                }
            ),
            "--profiles-dir",
            str(context.profiles_dir),
            "--no-version-check",
        ],
        cwd=context.project_dir,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _assertion_macros() -> str:
    return textwrap.dedent(
        """
        {% macro assert_iceberg_sync_integration(models, run_log_relation) %}
          {% for model in models %}
            {% set view_count = run_query('select count(*) from ' ~ model['view_relation']) %}
            {% set internal_count_sql %}
              select count(*) from {{ model['internal_relation'] }}
            {% endset %}
            {% set internal_count = run_query(internal_count_sql) %}
            {% set actual_view_rows = view_count.rows[0][0] | int %}
            {% set actual_internal_rows = internal_count.rows[0][0] | int %}
            {% if actual_view_rows != actual_internal_rows %}
              {% set message %}
                {{ model['view_relation'] }} count mismatch:
                view={{ actual_view_rows }}, internal={{ actual_internal_rows }}
              {% endset %}
              {{ exceptions.raise_compiler_error(message) }}
            {% endif %}
            {% if model.get('expected_rows') is not none %}
              {% if actual_view_rows != (model.get('expected_rows') | int) %}
                {% set message %}
                  {{ model['view_relation'] }} expected {{ model.get('expected_rows') }}
                  rows, got {{ actual_view_rows }}
                {% endset %}
                {{ exceptions.raise_compiler_error(message) }}
              {% endif %}
            {% elif actual_view_rows <= 0 %}
              {{ exceptions.raise_compiler_error(model['view_relation'] ~ ' produced no rows') }}
            {% endif %}

            {% set mode_sql %}
              select effective_mode
              from {{ run_log_relation }}
              where target_view = '{{ model['target_view'] | replace("'", "''") }}'
                and status = 'success'
              order by started_at
            {% endset %}
            {% set mode_rows = run_query(mode_sql) %}
            {% set actual_modes = [] %}
            {% for row in mode_rows.rows %}
              {% do actual_modes.append(row[0]) %}
            {% endfor %}
            {% if actual_modes != model.get('expected_modes') %}
              {% set message %}
                {{ model['view_relation'] }} expected modes
                {{ model.get('expected_modes') }}, got {{ actual_modes }}
              {% endset %}
              {{ exceptions.raise_compiler_error(message) }}
            {% endif %}

            {% if model.get('require_staging_table') %}
              {% set staging_sql %}
                select count(*)
                from {{ run_log_relation }}
                where target_view = '{{ model['target_view'] | replace("'", "''") }}'
                  and status = 'success'
                  and staging_table_reference is not null
              {% endset %}
              {% set staging_rows = run_query(staging_sql) %}
              {% if (staging_rows.rows[0][0] | int) == 0 %}
                {% set message %}
                  {{ model['view_relation'] }} did not record a staging table
                {% endset %}
                {{ exceptions.raise_compiler_error(message) }}
              {% endif %}
            {% endif %}
          {% endfor %}
        {% endmacro %}

        {% macro cleanup_iceberg_sync_integration(
            objects,
            procedure_relation,
            run_log_relation,
            handler_stage,
            drop_handler_stage
        ) %}
          {% for object in objects %}
            {% call statement('drop_view') %}
              DROP VIEW IF EXISTS {{ object['view_relation'] }}
            {% endcall %}
            {% call statement('drop_table') %}
              DROP ICEBERG TABLE IF EXISTS {{ object['internal_relation'] }}
            {% endcall %}
          {% endfor %}
          {% call statement('drop_procedure') %}
            DROP PROCEDURE IF EXISTS {{ procedure_relation }}(VARIANT)
          {% endcall %}
          {% call statement('drop_run_log') %}
            DROP TABLE IF EXISTS {{ run_log_relation }}
          {% endcall %}
          {% if drop_handler_stage %}
            {% call statement('drop_handler_stage') %}
              DROP STAGE IF EXISTS {{ handler_stage }}
            {% endcall %}
          {% endif %}
        {% endmacro %}
        """
    ).lstrip()


def _run_dbt(context: IntegrationContext, *args: str) -> None:
    subprocess.run(
        [
            _dbt_executable(),
            *args,
            "--profiles-dir",
            str(context.profiles_dir),
            "--no-version-check",
        ],
        cwd=context.project_dir,
        check=True,
        text=True,
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is required for integration tests")
    return value


def _required_int_env(name: str) -> int:
    return int(_required_env(name))


def _optional_int_env(name: str) -> int | None:
    value = os.environ.get(name)
    return int(value) if value else None


def _required_env_list(name: str) -> list[str]:
    value = _required_env(name)
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(parsed, str):
        parsed = [parsed]
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        pytest.skip(f"{name} must be a JSON string array or comma-separated string list")
    return parsed


def _dbt_executable() -> str:
    return str(Path(sys.executable).with_name("dbt"))


def _profile_yaml(*, database: str, schema: str) -> str:
    optional_lines = []
    for env_name, profile_name in (
        ("SNOWFLAKE_ROLE", "role"),
        ("SNOWFLAKE_WAREHOUSE", "warehouse"),
        ("SNOWFLAKE_PASSWORD", "password"),
        ("SNOWFLAKE_PRIVATE_KEY_PATH", "private_key_path"),
    ):
        value = os.environ.get(env_name)
        if value:
            optional_lines.append(f"      {profile_name}: {value}")

    return textwrap.dedent(
        f"""
        iceberg_sync_integration:
          target: test
          outputs:
            test:
              type: snowflake
              account: {_required_env("SNOWFLAKE_ACCOUNT")}
              user: {_required_env("SNOWFLAKE_USER")}
              authenticator: {os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")}
              database: {database}
              schema: {schema}
              threads: 1
        """
    ).lstrip() + "\n".join(optional_lines) + "\n"


def _export_prefix(context: IntegrationContext, model_name: str) -> str:
    return f"dbt_iceberg_sync_integration/{context.run_id}/{model_name}"


def _quoted_relation(database: str, schema: str, identifier: str) -> str:
    return ".".join(f'"{part}"' for part in (database, schema, identifier))


def _unquoted_relation(database: str, schema: str, identifier: str) -> str:
    return ".".join(part.upper() for part in (database, schema, identifier))


def _jstr(value: str) -> str:
    return json.dumps(value)


def _jlist(value: list[str]) -> str:
    return json.dumps(value)
