from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def test_dbt_extract_smoke(tmp_path: Path):
    if os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION") != "1":
        pytest.skip("set DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1 to run")

    run_id = uuid.uuid4().hex[:12]
    project_dir = tmp_path / "dbt_project"
    profiles_dir = tmp_path / "profiles"
    model_name = f"iceberg_sync_smoke_{run_id}"
    package_path = Path(
        os.environ.get(
            "DBT_SNOWFLAKE_ICEBERG_SYNC_PACKAGE_PATH",
            Path(__file__).resolve().parents[2],
        )
    )

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
    procedure_name = f"ICEBERG_SYNC_TEST_{run_id.upper()}"
    external_access_integration = _required_env(
        "DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_ACCESS_INTEGRATION"
    )
    secret_fqdn = _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_FQDN")
    secret_alias = os.environ.get(
        "DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_ALIAS",
        "google_cloud_service_account_credentials_json",
    )
    handler_stage_from_env = os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_HANDLER_STAGE")
    handler_stage = handler_stage_from_env or (
        f"{procedure_database}.{procedure_schema}.ICEBERG_SYNC_HANDLER_STAGE_{run_id.upper()}"
    )
    export_stage = _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_EXPORT_STAGE")
    export_prefix = f"dbt_iceberg_sync_integration/{run_id}/{model_name}"
    internal_relation = (
        f'"{snowflake_database}"."{snowflake_schema}"."__{model_name}"'
    )
    view_relation = f"{snowflake_database}.{snowflake_schema}.{model_name}"
    run_log_relation = (
        f"{procedure_database}.{procedure_schema}.ICEBERG_SYNC_RUN_LOG_{run_id.upper()}"
    )

    (project_dir / "models").mkdir(parents=True)
    (project_dir / "macros").mkdir(parents=True)
    profiles_dir.mkdir(parents=True)

    (project_dir / "packages.yml").write_text(
        f"packages:\n  - local: {package_path}\n",
        encoding="utf-8",
    )
    (project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            f"""
            name: iceberg_sync_integration_smoke
            version: 1.0.0
            config-version: 2
            profile: iceberg_sync_integration_smoke
            model-paths: [models]
            macro-paths: [macros]
            on-run-start:
              - "{{{{ dbt_snowflake_iceberg_sync.install_iceberg_sync_procedure() }}}}"
            vars:
              iceberg_sync:
                procedure_database: {procedure_database}
                procedure_schema: {procedure_schema}
                procedure_name: {procedure_name}
                run_log_table: {run_log_relation}
                handler_stage: {handler_stage}
                handler_stage_path: procedure
                handler_import_name: iceberg_sync_procedure_{run_id}
                handler_name: iceberg_sync_procedure_{run_id}.handler.main
                handler_local_path: dbt_packages/dbt_snowflake_iceberg_sync/procedure
                external_access_integrations:
                  - {external_access_integration}
                google_cloud_service_account_secret_fqdn: {secret_fqdn}
                google_cloud_service_account_secret_alias: {secret_alias}
            """
        ).lstrip(),
        encoding="utf-8",
    )
    (project_dir / "models" / f"{model_name}.sql").write_text(
        textwrap.dedent(
            f"""
            {{{{
              config(
                materialized='iceberg_sync',
                source_type='bigquery',
                materialization_strategy='full_refresh',
                bigquery_export_strategy='extract',
                google_cloud_project_id='{_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID")}',
                bigquery_dataset_id='{_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID")}',
                bigquery_table_id='{_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID")}',
                bigquery_location='{_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_LOCATION")}',
                bigquery_export_location='@{export_stage}/{export_prefix}',
                bigquery_export_predicate_type='none',
                iceberg_table_external_volume='{_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_VOLUME")}',
                iceberg_table_base_location='{export_prefix}'
              )
            }}}}
            """
        ).lstrip(),
        encoding="utf-8",
    )
    (project_dir / "macros" / "cleanup.sql").write_text(
        textwrap.dedent(
            """
            {% macro cleanup_iceberg_sync_smoke(
                view_relation,
                internal_relation,
                procedure_relation,
                run_log_relation,
                handler_stage,
                drop_handler_stage
            ) %}
              {% call statement('drop_view') %}
                DROP VIEW IF EXISTS {{ view_relation }}
              {% endcall %}
              {% call statement('drop_table') %}
                DROP ICEBERG TABLE IF EXISTS {{ internal_relation }}
              {% endcall %}
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
        ).lstrip(),
        encoding="utf-8",
    )
    (profiles_dir / "profiles.yml").write_text(
        _profile_yaml(
            database=snowflake_database,
            schema=snowflake_schema,
        ),
        encoding="utf-8",
    )

    try:
        _run_dbt(project_dir, profiles_dir, "deps")
        _run_dbt(project_dir, profiles_dir, "run", "--select", model_name)
    finally:
        cleanup_args = json.dumps(
            {
                "view_relation": view_relation,
                "internal_relation": internal_relation,
                "procedure_relation": f"{procedure_database}.{procedure_schema}.{procedure_name}",
                "run_log_relation": run_log_relation,
                "handler_stage": handler_stage,
                "drop_handler_stage": handler_stage_from_env is None,
            }
        )
        subprocess.run(
            [
                _dbt_executable(),
                "run-operation",
                "cleanup_iceberg_sync_smoke",
                "--args",
                cleanup_args,
                "--profiles-dir",
                str(profiles_dir),
                "--no-version-check",
            ],
            cwd=project_dir,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )


def _run_dbt(project_dir: Path, profiles_dir: Path, *args: str) -> None:
    subprocess.run(
        [_dbt_executable(), *args, "--profiles-dir", str(profiles_dir), "--no-version-check"],
        cwd=project_dir,
        check=True,
        text=True,
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is required for integration tests")
    return value


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
        iceberg_sync_integration_smoke:
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
