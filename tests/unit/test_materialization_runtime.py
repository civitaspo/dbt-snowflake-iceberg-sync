from __future__ import annotations

import json
import textwrap
from pathlib import Path

import agate
from dbt.adapters.contracts.connection import AdapterResponse, ConnectionState
from dbt.adapters.snowflake.connections import SnowflakeConnectionManager
from dbt.cli.main import dbtRunner
from dbt_common.clients.agate_helper import empty_table


def test_iceberg_sync_dbt_run_executes_procedure_as_main_statement(
    tmp_path: Path,
    monkeypatch,
):
    run_result, executed_sql = _run_dbt_iceberg_sync_model(
        tmp_path,
        monkeypatch,
        {"status": "success"},
    )

    call_statements = [
        call for call in executed_sql if _normalize_sql(call["sql"]).startswith("call ")
    ]

    assert run_result.success
    assert len(call_statements) == 1
    assert call_statements[0]["fetch"] is True
    assert call_statements[0]["auto_begin"] is True


def test_iceberg_sync_dbt_run_surfaces_procedure_failure(
    tmp_path: Path,
    monkeypatch,
):
    run_result, _ = _run_dbt_iceberg_sync_model(
        tmp_path,
        monkeypatch,
        {"status": "failure", "error_message": "procedure exploded"},
    )

    message = run_result.result.results[0].message

    assert not run_result.success
    assert "procedure exploded" in message
    assert "main is not being called during running model" not in message


def _run_dbt_iceberg_sync_model(
    tmp_path: Path,
    monkeypatch,
    procedure_result: dict[str, object],
):
    repo_root = Path(__file__).resolve().parents[2]
    project_dir = tmp_path / "project"
    profiles_dir = project_dir / "profiles"
    models_dir = project_dir / "models"
    profiles_dir.mkdir(parents=True)
    models_dir.mkdir()

    (project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            f"""
            name: iceberg_sync_regression
            version: "1.0"
            config-version: 2
            profile: iceberg_sync_regression
            model-paths: ["models"]
            packages-install-path: dbt_packages
            vars:
              iceberg_sync:
                handler_local_path: "{repo_root / "procedure"}"
                google_cloud_service_account_secret_fqdn: test_db.test_schema.test_secret
                external_access_integrations: test_integration
            """
        ),
        encoding="utf-8",
    )
    (project_dir / "packages.yml").write_text(
        textwrap.dedent(
            f"""
            packages:
              - local: {repo_root}
            """
        ),
        encoding="utf-8",
    )
    (profiles_dir / "profiles.yml").write_text(
        textwrap.dedent(
            """
            iceberg_sync_regression:
              target: ci
              outputs:
                ci:
                  type: snowflake
                  account: test-account
                  user: test-user
                  password: test-password
                  role: TEST_ROLE
                  warehouse: TEST_WAREHOUSE
                  database: TEST_DATABASE
                  schema: TEST_SCHEMA
                  threads: 1
            """
        ),
        encoding="utf-8",
    )
    (models_dir / "model.sql").write_text(
        textwrap.dedent(
            """
            {{ config(
                materialized='iceberg_sync',
                google_cloud_project_id='project',
                bigquery_dataset_id='dataset',
                bigquery_table_id='table',
                bigquery_location='US',
                bigquery_export_location='@test_db.test_schema.test_stage/path',
                snowflake_stage='test_db.test_schema.test_stage',
                iceberg_table_external_volume='test_volume'
            ) }}
            select 1 as id
            """
        ),
        encoding="utf-8",
    )

    executed_sql: list[dict[str, object]] = []

    class FakeHandle:
        def close(self):
            return None

    def fake_open(cls, connection):
        connection.state = ConnectionState.OPEN
        connection.handle = FakeHandle()
        return connection

    def fake_execute(self, sql, auto_begin=False, fetch=False, limit=None):
        executed_sql.append(
            {
                "sql": sql,
                "auto_begin": auto_begin,
                "fetch": fetch,
                "limit": limit,
            }
        )
        normalized = _normalize_sql(sql)
        response = AdapterResponse(_message="SUCCESS", code="SUCCESS", rows_affected=1)
        if normalized.startswith("call "):
            return response, agate.Table([[json.dumps(procedure_result)]], ["RESULT"])
        if normalized.startswith("show objects"):
            return response, _show_objects_table()
        if normalized.startswith("show terse schemas"):
            return response, agate.Table([["TEST_SCHEMA"]], ["name"])
        return response, empty_table()

    monkeypatch.setattr(SnowflakeConnectionManager, "open", classmethod(fake_open))
    monkeypatch.setattr(SnowflakeConnectionManager, "execute", fake_execute)

    runner = dbtRunner()
    deps_result = runner.invoke(
        ["deps", "--project-dir", str(project_dir), "--profiles-dir", str(profiles_dir)]
    )
    assert deps_result.success

    run_result = runner.invoke(
        [
            "run",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--no-version-check",
        ]
    )

    return run_result, executed_sql


def _show_objects_table() -> agate.Table:
    return agate.Table(
        [["TEST_DATABASE", "TEST_SCHEMA", "UNRELATED", "TABLE", "N", "N"]],
        ["database_name", "schema_name", "name", "kind", "is_dynamic", "is_iceberg"],
    )


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split()).lower()
