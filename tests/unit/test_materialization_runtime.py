from __future__ import annotations

import json
import textwrap
from pathlib import Path

import agate
from dbt.adapters.contracts.connection import AdapterResponse, ConnectionState
from dbt.adapters.snowflake.connections import SnowflakeConnectionManager
from dbt.cli.main import dbtRunner
from dbt_common.clients.agate_helper import empty_table


def test_iceberg_sync_dbt_run_orchestrates_snowflake_work_in_dbt(
    tmp_path: Path,
    monkeypatch,
):
    run_result, executed_sql = _run_dbt_iceberg_sync_model(
        tmp_path,
        monkeypatch,
        [
            {"status": "running", "export_state": {"phase": "extract"}},
            _successful_export_result(),
        ],
    )

    normalized_statements = [_normalize_sql(call["sql"]) for call in executed_sql]

    assert run_result.success
    assert any(sql.startswith("call ") for sql in normalized_statements)
    assert any("system$wait" in sql for sql in normalized_statements)
    assert any("create iceberg table if not exists" in sql for sql in normalized_statements)
    assert any(sql.startswith("describe table") for sql in normalized_statements)
    assert not any(sql.startswith("drop iceberg table") for sql in normalized_statements)
    assert not any(sql.startswith("execute immediate ") for sql in normalized_statements)
    assert any("begin; delete from" in sql for sql in normalized_statements)
    assert any("copy into" in sql for sql in normalized_statements)
    assert any("commit;" in sql for sql in normalized_statements)
    assert any("load_mode = add_files_copy" in sql for sql in normalized_statements)
    assert any("create or replace view" in sql for sql in normalized_statements)
    assert any(
        "insert into" in sql and "iceberg_sync_run_log" in sql
        for sql in normalized_statements
    )
    assert not any("000603" in sql or "300005" in sql for sql in normalized_statements)


def test_iceberg_sync_dbt_run_surfaces_procedure_failure(
    tmp_path: Path,
    monkeypatch,
):
    run_result, _ = _run_dbt_iceberg_sync_model(
        tmp_path,
        monkeypatch,
        [{"status": "failure", "error_message": "procedure exploded"}],
    )

    message = run_result.result.results[0].message

    assert not run_result.success
    assert "procedure exploded" in message
    assert "main is not being called during running model" not in message


def test_iceberg_sync_dbt_run_skips_when_export_is_skipped(
    tmp_path: Path,
    monkeypatch,
):
    run_result, executed_sql = _run_dbt_iceberg_sync_model(
        tmp_path,
        monkeypatch,
        [
            {
                "status": "skipped",
                "skip_reason": "BigQuery extract source table was not found",
                "export_result": {
                    "schema_fields": [],
                    "segments": [],
                    "job_references": [],
                    "staging_table_reference": None,
                    "columns": [],
                    "view_columns": [],
                },
            }
        ],
        model_config_extra="bigquery_extract_skip_missing_tables=true",
    )

    normalized_statements = [_normalize_sql(call["sql"]) for call in executed_sql]

    assert run_result.success
    assert any('"skip_missing_tables": true' in call["sql"] for call in executed_sql)
    assert any("insert into" in sql and "'skipped'" in sql for sql in normalized_statements)
    assert not any("system$wait" in sql for sql in normalized_statements)
    assert not any("create iceberg table" in sql for sql in normalized_statements)
    assert not any("copy into" in sql for sql in normalized_statements)
    assert not any("create or replace view" in sql for sql in normalized_statements)


def test_iceberg_sync_dbt_run_rejects_invalid_outer_retry_number(
    tmp_path: Path,
    monkeypatch,
):
    run_result, executed_sql = _run_dbt_iceberg_sync_model(
        tmp_path,
        monkeypatch,
        {"status": "success"},
        model_config_extra="iceberg_sync_retry_max_attempts='not-number'",
    )

    assert not run_result.success
    assert not any(
        _normalize_sql(call["sql"]).startswith("execute immediate ")
        for call in executed_sql
    )


def _run_dbt_iceberg_sync_model(
    tmp_path: Path,
    monkeypatch,
    procedure_results: list[dict[str, object]],
    *,
    model_config_extra: str = "",
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
    extra_config_sql = ""
    if model_config_extra:
        extra_config_sql = ",\n" + textwrap.indent(
            model_config_extra.strip(),
            "                ",
        )

    (models_dir / "model.sql").write_text(
        textwrap.dedent(
            f"""
            {{{{ config(
                materialized='iceberg_sync',
                google_cloud_project_id='project',
                bigquery_dataset_id='dataset',
                bigquery_table_id='table',
                bigquery_location='US',
                bigquery_export_location='@test_db.test_schema.test_stage/path',
                snowflake_stage='test_db.test_schema.test_stage',
                iceberg_table_external_volume='test_volume'{extra_config_sql}
            ) }}}}
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

    procedure_queue = list(procedure_results)

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
        if normalized.startswith("desc stage"):
            return response, _desc_stage_table()
        if normalized.startswith("describe table"):
            return response, _describe_internal_table()
        if normalized.startswith("call system$wait"):
            return response, empty_table()
        if normalized.startswith("call "):
            result = procedure_queue.pop(0)
            return response, agate.Table([[json.dumps(result)]], ["RESULT"])
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


def _desc_stage_table() -> agate.Table:
    return agate.Table(
        [["URL", "gcs://bucket/dbt"]],
        ["property", "property_value"],
    )


def _describe_internal_table() -> agate.Table:
    return agate.Table(
        [["OrderID", "NUMBER(19,0)", "COLUMN", "Y"]],
        ["name", "type", "kind", "null?"],
    )


def _successful_export_result() -> dict[str, object]:
    return {
        "status": "success",
        "export_result": {
            "schema_fields": [{"name": "OrderID", "type": "INT64"}],
            "segments": [{"destination_uri": "gs://bucket/dbt/run/segment-*.parquet"}],
            "job_references": [{"projectId": "project", "location": "US", "jobId": "job"}],
            "staging_table_reference": None,
            "columns": [
                {
                    "source_name": "OrderID",
                    "snowflake_type": "BIGINT",
                    "nullable": True,
                    "fields": [],
                    "ddl": '"OrderID" BIGINT',
                }
            ],
            "view_columns": [{"source_name": "OrderID", "alias": "order_id"}],
        },
    }


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split()).lower()
