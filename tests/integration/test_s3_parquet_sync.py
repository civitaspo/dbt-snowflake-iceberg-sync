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


def _nested_field(name: str, snowflake_type: str) -> dict[str, Any]:
    return {
        "source_name": name,
        "snowflake_type": snowflake_type,
        "nullable": True,
        "fields": [],
    }


@dataclass(frozen=True)
class S3IntegrationContext:
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
    s3_stage: str
    external_volume: str
    parquet_file_format: str

    @property
    def procedure_relation(self) -> str:
        return f"{self.procedure_database}.{self.procedure_schema}.{self.procedure_name}"


def test_s3_parquet_full_refresh_smoke(tmp_path: Path):
    """Full refresh without declared columns (INFER_SCHEMA path)."""

    context = _s3_context(tmp_path, "s3_smoke")
    model_name = f"iceberg_sync_s3_smoke_{context.run_id}"
    location_prefix = f"iceberg_sync_s3/{context.run_id}/{model_name}"
    models = {
        model_name: _s3_model_sql(
            context,
            model_name=model_name,
            location=f"@{context.s3_stage}/{location_prefix}/full",
            base_location=location_prefix,
        )
    }
    _write_s3_project(context, models)
    try:
        _run_dbt(context, "deps")
        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/full",
            select_sql=(
                "SELECT 1::NUMBER AS \"OrderID\", 'alice'::VARCHAR AS \"CustomerName\" "
                "UNION ALL SELECT 2, 'bob'"
            ),
        )
        _run_dbt(context, "run", "--select", model_name)
        _assert_row_count(context, model_name, expected_rows=2)
        _assert_run_log_status(context, model_name, status="success", mode="full_refresh")
    finally:
        _cleanup(context, [model_name], location_prefix)


def test_s3_parquet_pattern_filter(tmp_path: Path):
    context = _s3_context(tmp_path, "s3_pattern")
    model_name = f"iceberg_sync_s3_pattern_{context.run_id}"
    location_prefix = f"iceberg_sync_s3/{context.run_id}/{model_name}"
    models = {
        model_name: _s3_model_sql(
            context,
            model_name=model_name,
            location=f"@{context.s3_stage}/{location_prefix}/data",
            base_location=location_prefix,
            extra_config={"s3_parquet_file_pattern": ".*keep.*[.]parquet"},
        )
    }
    _write_s3_project(context, models)
    try:
        _run_dbt(context, "deps")
        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/data",
            select_sql="SELECT 1::NUMBER AS \"OrderID\"",
            file_name_prefix="keep",
        )
        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/data",
            select_sql="SELECT 2::NUMBER AS \"OrderID\"",
            file_name_prefix="drop",
        )
        _run_dbt(context, "run", "--select", model_name)
        _assert_row_count(context, model_name, expected_rows=1)
    finally:
        _cleanup(context, [model_name], location_prefix)


def test_s3_parquet_incremental_delete_copy_and_force_reload(tmp_path: Path):
    context = _s3_context(tmp_path, "s3_incr")
    model_name = f"iceberg_sync_s3_incr_{context.run_id}"
    location_prefix = f"iceberg_sync_s3/{context.run_id}/{model_name}"
    models = {
        model_name: _s3_model_sql(
            context,
            model_name=model_name,
            location=f"@{context.s3_stage}/{location_prefix}",
            base_location=location_prefix,
            materialization_strategy="incremental",
            full_refresh_paths=["dt=2026-01-01", "dt=2026-01-02"],
            incremental_paths=["dt=2026-01-02"],
            incremental_predicate='"EventDate" = DATE \'2026-01-02\'',
        )
    }
    _write_s3_project(context, models)
    try:
        _run_dbt(context, "deps")
        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/dt=2026-01-01",
            select_sql=(
                "SELECT 1::NUMBER AS \"OrderID\", '2026-01-01'::DATE AS \"EventDate\""
            ),
        )
        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/dt=2026-01-02",
            select_sql=(
                "SELECT 2::NUMBER AS \"OrderID\", '2026-01-02'::DATE AS \"EventDate\""
            ),
        )
        _run_dbt(context, "run", "--select", model_name)
        _assert_row_count(context, model_name, expected_rows=2)
        _run_dbt(context, "run", "--select", model_name)
        _assert_row_count(context, model_name, expected_rows=2)
        _run_dbt(context, "run", "--select", model_name)
        _assert_row_count(context, model_name, expected_rows=2)
        _assert_run_log_modes(
            context,
            model_name,
            expected_modes=["full_refresh", "incremental", "incremental"],
        )
    finally:
        _cleanup(context, [model_name], location_prefix)


def test_s3_parquet_empty_location_skip_and_fail(tmp_path: Path):
    context = _s3_context(tmp_path, "s3_empty")
    skip_model = f"iceberg_sync_s3_empty_skip_{context.run_id}"
    fail_model = f"iceberg_sync_s3_empty_fail_{context.run_id}"
    location_prefix = f"iceberg_sync_s3/{context.run_id}/empty"
    models = {
        skip_model: _s3_model_sql(
            context,
            model_name=skip_model,
            location=f"@{context.s3_stage}/{location_prefix}/missing",
            base_location=f"{location_prefix}/skip",
            extra_config={"s3_parquet_skip_missing_location": True},
        ),
        fail_model: _s3_model_sql(
            context,
            model_name=fail_model,
            location=f"@{context.s3_stage}/{location_prefix}/missing",
            base_location=f"{location_prefix}/fail",
        ),
    }
    _write_s3_project(context, models)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run", "--select", skip_model)
        _assert_run_log_status(context, skip_model, status="skipped", mode="full_refresh")
        failed = _run_dbt(context, "run", "--select", fail_model, check=False)
        assert failed.returncode != 0
    finally:
        _cleanup(context, [skip_model, fail_model], location_prefix)


def test_s3_parquet_additive_schema_evolution(tmp_path: Path):
    context = _s3_context(tmp_path, "s3_schema")
    model_name = f"iceberg_sync_s3_schema_{context.run_id}"
    location_prefix = f"iceberg_sync_s3/{context.run_id}/{model_name}"
    models_v1 = {
        model_name: _s3_model_sql(
            context,
            model_name=model_name,
            location=f"@{context.s3_stage}/{location_prefix}/v1",
            base_location=location_prefix,
        )
    }
    _write_s3_project(context, models_v1)
    try:
        _run_dbt(context, "deps")
        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/v1",
            select_sql="SELECT 1::NUMBER AS \"OrderID\"",
        )
        _run_dbt(context, "run", "--select", model_name)

        _unload_parquet_fixture(
            context,
            stage_prefix=f"{location_prefix}/v2",
            select_sql=(
                "SELECT 2::NUMBER AS \"OrderID\", 'extra'::VARCHAR AS \"CustomerName\""
            ),
        )
        models_v2 = {
            model_name: _s3_model_sql(
                context,
                model_name=model_name,
                location=f"@{context.s3_stage}/{location_prefix}/v2",
                base_location=location_prefix,
            )
        }
        _write_s3_project(context, models_v2)
        _run_dbt(context, "run", "--select", model_name)
        _assert_row_count(context, model_name, expected_rows=1)
    finally:
        _cleanup(context, [model_name], location_prefix)


def test_s3_parquet_rejects_incremental_path_without_predicate(tmp_path: Path):
    context = _s3_context(tmp_path, "s3_invalid")
    model_name = f"iceberg_sync_s3_invalid_{context.run_id}"
    location_prefix = f"iceberg_sync_s3/{context.run_id}/{model_name}"
    models = {
        model_name: _s3_model_sql(
            context,
            model_name=model_name,
            location=f"@{context.s3_stage}/{location_prefix}",
            base_location=location_prefix,
            materialization_strategy="incremental",
            incremental_paths=["dt=2026-01-02"],
        )
    }
    _write_s3_project(context, models)
    try:
        _run_dbt(context, "deps")
        failed = _run_dbt(context, "run", "--select", model_name, check=False)
        assert failed.returncode != 0
        combined = ((failed.stdout or "") + (failed.stderr or "")).lower()
        assert "incremental_predicate" in combined
    finally:
        _cleanup(context, [model_name], location_prefix)


def test_s3_nested_object_schema_evolution_matrix(tmp_path: Path):
    """Exercise nested OBJECT evolution against Snowflake-managed Iceberg tables."""

    context = _s3_context(tmp_path, "s3_nested")
    location_prefix = f"iceberg_sync_s3/{context.run_id}/nested_evolution"
    _write_s3_project(context, {})
    cases = [
        {
            "name": "nested_add",
            "initial_ddl": (
                '"payload" OBJECT("alpha" VARCHAR, "beta" BIGINT)'
            ),
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": (
                        'OBJECT("alpha" VARCHAR, "beta" BIGINT, "gamma" VARCHAR)'
                    ),
                    "nullable": True,
                    "fields": [
                        _nested_field("alpha", "VARCHAR"),
                        _nested_field("beta", "BIGINT"),
                        _nested_field("gamma", "VARCHAR"),
                    ],
                }
            ],
            "expect_success": True,
            "expect_type_fragment": "GAMMA",
            "expect_warning_substring": None,
            "expect_altered": True,
        },
        {
            "name": "nested_reorder",
            "initial_ddl": (
                '"payload" OBJECT("alpha" VARCHAR, "beta" BIGINT)'
            ),
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": (
                        'OBJECT("beta" BIGINT, "alpha" VARCHAR)'
                    ),
                    "nullable": True,
                    "fields": [
                        _nested_field("beta", "BIGINT"),
                        _nested_field("alpha", "VARCHAR"),
                    ],
                }
            ],
            "expect_success": True,
            # DESCRIBE spells BIGINT as NUMBER(19,0); order proves reorder ran.
            "expect_type_fragment": "BETA NUMBER(19,0), ALPHA VARCHAR",
            "expect_warning_substring": None,
            "expect_altered": True,
        },
        {
            "name": "nested_widen",
            "initial_ddl": '"payload" OBJECT("item_count" INTEGER)',
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": 'OBJECT("item_count" BIGINT)',
                    "nullable": True,
                    "fields": [
                        _nested_field("item_count", "BIGINT"),
                    ],
                }
            ],
            "expect_success": True,
            "expect_type_fragment": "ITEM_COUNT NUMBER(19,0)",
            "expect_warning_substring": None,
            "expect_altered": True,
        },
        {
            "name": "keep_missing_warn",
            "initial_ddl": (
                '"payload" OBJECT("alpha" VARCHAR, "legacy_note" VARCHAR)'
            ),
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": 'OBJECT("alpha" VARCHAR)',
                    "nullable": True,
                    "fields": [
                        _nested_field("alpha", "VARCHAR"),
                    ],
                }
            ],
            "expect_success": True,
            "expect_type_fragment": "LEGACY_NOTE",
            # DESCRIBE keeps unquoted nested identifiers uppercase.
            "expect_warning_substring": (
                "keeping nested field payload.LEGACY_NOTE"
            ),
            "expect_altered": False,
        },
        {
            "name": "deep_nest_add",
            "initial_ddl": (
                '"payload" OBJECT("outer" OBJECT("inner_a" VARCHAR))'
            ),
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": (
                        'OBJECT("outer" OBJECT("inner_a" VARCHAR, "inner_b" VARCHAR))'
                    ),
                    "nullable": True,
                    "fields": [
                        {
                            "source_name": "outer",
                            "snowflake_type": (
                                'OBJECT("inner_a" VARCHAR, "inner_b" VARCHAR)'
                            ),
                            "nullable": True,
                            "fields": [
                                _nested_field("inner_a", "VARCHAR"),
                                _nested_field("inner_b", "VARCHAR"),
                            ],
                        }
                    ],
                }
            ],
            "expect_success": True,
            "expect_type_fragment": "INNER_B",
            "expect_warning_substring": None,
            "expect_altered": True,
        },
        {
            "name": "array_object_add",
            "initial_ddl": '"items" ARRAY(OBJECT("sku" VARCHAR))',
            "desired_columns": [
                {
                    "source_name": "items",
                    "snowflake_type": 'ARRAY(OBJECT("sku" VARCHAR, "qty" BIGINT))',
                    "nullable": True,
                    "fields": [
                        _nested_field("sku", "VARCHAR"),
                        _nested_field("qty", "BIGINT"),
                    ],
                }
            ],
            "expect_success": True,
            "expect_type_fragment": "QTY",
            "expect_warning_substring": None,
            "expect_altered": True,
        },
        {
            "name": "incompatible_type",
            "initial_ddl": '"payload" OBJECT("alpha" VARCHAR)',
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": 'OBJECT("alpha" BOOLEAN)',
                    "nullable": True,
                    "fields": [
                        _nested_field("alpha", "BOOLEAN"),
                    ],
                }
            ],
            "expect_success": False,
            "expect_type_fragment": None,
            "expect_warning_substring": None,
            "expect_altered": False,
        },
        {
            "name": "combined_add_reorder_widen",
            "initial_ddl": (
                '"payload" OBJECT("alpha" INTEGER, "beta" VARCHAR)'
            ),
            "desired_columns": [
                {
                    "source_name": "payload",
                    "snowflake_type": (
                        'OBJECT("beta" VARCHAR, "alpha" BIGINT, "gamma" VARCHAR)'
                    ),
                    "nullable": True,
                    "fields": [
                        _nested_field("beta", "VARCHAR"),
                        _nested_field("alpha", "BIGINT"),
                        _nested_field("gamma", "VARCHAR"),
                    ],
                }
            ],
            "expect_success": True,
            "expect_type_fragment": "GAMMA",
            "expect_warning_substring": None,
            "expect_altered": True,
        },
    ]
    try:
        _run_dbt(context, "deps")
        _run_dbt(
            context,
            "run-operation",
            "install_iceberg_sync_procedure",
        )
        for case in cases:
            case_table = _quoted_relation(
                context.snowflake_database,
                context.snowflake_schema,
                f"__ICEBERG_SYNC_NESTED_{case['name'].upper()}_{context.run_id.upper()}",
            )
            result = _run_dbt(
                context,
                "run-operation",
                "run_nested_schema_evolution_case",
                "--args",
                json.dumps(
                    {
                        "procedure_relation": context.procedure_relation,
                        "table_relation": case_table,
                        "external_volume": context.external_volume,
                        "base_location": f"{location_prefix}/{case['name']}",
                        "initial_ddl": case["initial_ddl"],
                        "desired_columns": case["desired_columns"],
                        "expect_success": case["expect_success"],
                        "expect_type_fragment": case["expect_type_fragment"],
                        "expect_warning_substring": case["expect_warning_substring"],
                        "expect_altered": case["expect_altered"],
                    }
                ),
                check=False,
            )
            if result.returncode != 0:
                raise AssertionError(
                    f"nested evolution case {case['name']} failed:\n"
                    f"{result.stdout}\n{result.stderr}"
                )
    finally:
        _run_dbt(
            context,
            "run-operation",
            "cleanup_nested_schema_evolution_tables",
            "--args",
            json.dumps(
                {
                    "table_relations": [
                        _quoted_relation(
                            context.snowflake_database,
                            context.snowflake_schema,
                            f"__ICEBERG_SYNC_NESTED_{case['name'].upper()}_{context.run_id.upper()}",
                        )
                        for case in cases
                    ],
                    "procedure_relation": context.procedure_relation,
                    "run_log_relation": context.run_log_relation,
                    "handler_stage": context.handler_stage,
                    "drop_handler_stage": context.handler_stage_from_env is None,
                    "parquet_file_format": context.parquet_file_format,
                    "stage_fqn": context.s3_stage,
                    "stage_prefix": location_prefix,
                }
            ),
            check=False,
        )


def _s3_context(tmp_path: Path, prefix: str) -> S3IntegrationContext:
    if os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION") != "1":
        pytest.skip("set DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1 to run")
    if not os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_S3_PARQUET_STAGE"):
        pytest.skip(
            "Set DBT_SNOWFLAKE_ICEBERG_SYNC_S3_PARQUET_STAGE to an S3-backed named stage "
            "to run S3 Parquet integration tests."
        )

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
    return S3IntegrationContext(
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
        s3_stage=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_S3_PARQUET_STAGE"),
        external_volume=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_VOLUME"),
        parquet_file_format=(
            f"{procedure_database}.{procedure_schema}."
            f"ICEBERG_SYNC_PARQUET_FILE_FORMAT_{run_id.upper()}"
        ),
    )


def _write_s3_project(context: S3IntegrationContext, models: dict[str, str]) -> None:
    (context.project_dir / "models").mkdir(parents=True, exist_ok=True)
    (context.project_dir / "macros").mkdir(parents=True, exist_ok=True)
    context.profiles_dir.mkdir(parents=True, exist_ok=True)

    (context.project_dir / "packages.yml").write_text(
        f"packages:\n  - local: {context.package_path}\n",
        encoding="utf-8",
    )
    (context.project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            f"""
            name: iceberg_sync_s3_integration_{context.run_id}
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
                handler_local_path: {json.dumps(str(context.package_path / "procedure"))}
                parquet_file_format: {context.parquet_file_format}
            """
        ).lstrip(),
        encoding="utf-8",
    )
    for model_name, sql in models.items():
        (context.project_dir / "models" / f"{model_name}.sql").write_text(sql, encoding="utf-8")
    (context.project_dir / "macros" / "s3_integration_helpers.sql").write_text(
        _helper_macros(),
        encoding="utf-8",
    )
    (context.profiles_dir / "profiles.yml").write_text(
        _profile_yaml(
            database=context.snowflake_database,
            schema=context.snowflake_schema,
        ),
        encoding="utf-8",
    )


def _s3_model_sql(
    context: S3IntegrationContext,
    *,
    model_name: str,
    location: str,
    base_location: str,
    materialization_strategy: str = "full_refresh",
    full_refresh_paths: list[str] | None = None,
    incremental_paths: list[str] | None = None,
    incremental_predicate: str | None = None,
    extra_config: dict[str, Any] | None = None,
) -> str:
    incremental_setup = ""
    incremental_config = ""
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
                'incremental_strategy': 'delete+copy',
                'incremental_predicate': iceberg_sync_incremental_predicate | trim,
            """
        )
    extra_config_sql = ""
    if extra_config:
        extra_config_sql = "".join(
            f"                '{key}': {_jinja_value(value)},\n"
            for key, value in extra_config.items()
        )
    return textwrap.dedent(
        f"""
        {incremental_setup}
        {{{{
          config(
            materialized='iceberg_sync',
            meta={{
              'iceberg_sync': {{
                'source_type': 's3_parquet',
                'materialization_strategy': {_jstr(materialization_strategy)},
        {textwrap.indent(incremental_config, "        ").rstrip()}
                's3_parquet_location': {_jstr(location)},
                's3_parquet_full_refresh_paths': {_jlist(full_refresh_paths or [''])},
                's3_parquet_incremental_paths': {_jlist(incremental_paths or [''])},
                'iceberg_table_external_volume': {_jstr(context.external_volume)},
                'iceberg_table_base_location': {_jstr(base_location)},
{extra_config_sql.rstrip()}
              }}
            }}
          )
        }}}}
        """
    ).lstrip()


def _unload_parquet_fixture(
    context: S3IntegrationContext,
    *,
    stage_prefix: str,
    select_sql: str,
    file_name_prefix: str = "part",
) -> None:
    _run_dbt(
        context,
        "run-operation",
        "unload_s3_parquet_fixture",
        "--args",
        json.dumps(
            {
                "stage_fqn": context.s3_stage,
                "stage_prefix": stage_prefix,
                "select_sql": select_sql,
                "file_name_prefix": file_name_prefix,
            }
        ),
    )


def _assert_row_count(
    context: S3IntegrationContext, model_name: str, *, expected_rows: int
) -> None:
    _run_dbt(
        context,
        "run-operation",
        "assert_s3_parquet_row_count",
        "--args",
        json.dumps(
            {
                "view_relation": _unquoted_relation(
                    context.snowflake_database,
                    context.snowflake_schema,
                    model_name,
                ),
                "expected_rows": expected_rows,
            }
        ),
    )


def _assert_run_log_status(
    context: S3IntegrationContext,
    model_name: str,
    *,
    status: str,
    mode: str,
) -> None:
    _run_dbt(
        context,
        "run-operation",
        "assert_s3_parquet_run_log",
        "--args",
        json.dumps(
            {
                "run_log_relation": context.run_log_relation,
                "target_view": _quoted_relation(
                    context.snowflake_database,
                    context.snowflake_schema,
                    model_name,
                ),
                "expected_status": status,
                "expected_mode": mode,
            }
        ),
    )


def _assert_run_log_modes(
    context: S3IntegrationContext,
    model_name: str,
    *,
    expected_modes: list[str],
) -> None:
    _run_dbt(
        context,
        "run-operation",
        "assert_s3_parquet_run_log_modes",
        "--args",
        json.dumps(
            {
                "run_log_relation": context.run_log_relation,
                "target_view": _quoted_relation(
                    context.snowflake_database,
                    context.snowflake_schema,
                    model_name,
                ),
                "expected_modes": expected_modes,
            }
        ),
    )


def _cleanup(
    context: S3IntegrationContext,
    model_names: list[str],
    location_prefix: str,
) -> None:
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
            "cleanup_s3_parquet_integration",
            "--args",
            json.dumps(
                {
                    "objects": objects,
                    "procedure_relation": context.procedure_relation,
                    "run_log_relation": context.run_log_relation,
                    "handler_stage": context.handler_stage,
                    "drop_handler_stage": context.handler_stage_from_env is None,
                    "parquet_file_format": context.parquet_file_format,
                    "stage_fqn": context.s3_stage,
                    "stage_prefix": location_prefix,
                }
            ),
            "--profiles-dir",
            str(context.profiles_dir),
            "--no-version-check",
            "--project-dir",
            str(context.project_dir),
        ],
        check=False,
        cwd=context.project_dir,
        env=_dbt_env(),
    )


def _run_dbt(
    context: S3IntegrationContext,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    command = [
        _dbt_executable(),
        *args,
        "--profiles-dir",
        str(context.profiles_dir),
        "--no-version-check",
        "--project-dir",
        str(context.project_dir),
    ]
    return subprocess.run(
        command,
        check=check,
        cwd=context.project_dir,
        env=_dbt_env(),
        text=True,
        capture_output=True,
    )


def _helper_macros() -> str:
    return textwrap.dedent(
        """
        {% macro unload_s3_parquet_fixture(
          stage_fqn,
          stage_prefix,
          select_sql,
          file_name_prefix='part'
        ) %}
          {% set destination = '@' ~ stage_fqn ~ '/' ~ stage_prefix ~ '/' ~ file_name_prefix %}
          {% call statement('unload_s3_parquet_fixture', auto_begin=False) %}
            COPY INTO {{ destination }}
            FROM ({{ select_sql }})
            FILE_FORMAT = (TYPE = PARQUET)
            OVERWRITE = TRUE
          {% endcall %}
        {% endmacro %}

        {% macro assert_s3_parquet_row_count(view_relation, expected_rows) %}
          {% set result = run_query('SELECT COUNT(*) AS row_count FROM ' ~ view_relation) %}
          {% set actual = result.columns[0].values()[0] | int %}
          {% if actual != expected_rows %}
            {{ exceptions.raise_compiler_error(
              'expected ' ~ expected_rows ~ ' rows, found ' ~ actual ~ ' in ' ~ view_relation
            ) }}
          {% endif %}
        {% endmacro %}

        {% macro assert_s3_parquet_run_log(
          run_log_relation,
          target_view,
          expected_status,
          expected_mode
        ) %}
          {% set result = run_query(
            "SELECT status, effective_mode FROM " ~ run_log_relation ~
            " WHERE target_view = '" ~ target_view ~ "'" ~
            " ORDER BY finished_at DESC LIMIT 1"
          ) %}
          {% if result.rows | length == 0 %}
            {{ exceptions.raise_compiler_error('missing run log for ' ~ target_view) }}
          {% endif %}
          {% set status = result.columns[0].values()[0] %}
          {% set mode = result.columns[1].values()[0] %}
          {% if status != expected_status or mode != expected_mode %}
            {{ exceptions.raise_compiler_error(
              'expected status/mode ' ~ expected_status ~ '/' ~ expected_mode ~
              ', found ' ~ status ~ '/' ~ mode
            ) }}
          {% endif %}
        {% endmacro %}

        {% macro assert_s3_parquet_run_log_modes(run_log_relation, target_view, expected_modes) %}
          {% set result = run_query(
            "SELECT effective_mode FROM " ~ run_log_relation ~
            " WHERE target_view = '" ~ target_view ~ "'" ~
            " ORDER BY finished_at ASC"
          ) %}
          {% set actual = result.columns[0].values() | list %}
          {% if actual != expected_modes %}
            {{ exceptions.raise_compiler_error(
              'expected modes ' ~ expected_modes ~ ', found ' ~ actual
            ) }}
          {% endif %}
        {% endmacro %}

        {% macro run_nested_schema_evolution_case(
          procedure_relation,
          table_relation,
          external_volume,
          base_location,
          initial_ddl,
          desired_columns,
          expect_success,
          expect_type_fragment,
          expect_warning_substring,
          expect_altered
        ) %}
          {% call statement('drop_nested_case_table', auto_begin=False) %}
            DROP ICEBERG TABLE IF EXISTS {{ table_relation }}
          {% endcall %}
          {% call statement('create_nested_case_table', auto_begin=False) %}
            CREATE ICEBERG TABLE {{ table_relation }} (
              {{ initial_ddl }}
            )
            EXTERNAL_VOLUME = '{{ external_volume }}'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = '{{ base_location }}'
            ICEBERG_VERSION = 3
          {% endcall %}
          {% set relation_parts = table_relation.replace('"', '').split('.') %}
          {% set action_payload = {
            'action': 'evolve_schema',
            'internal_relation': {
              'database': relation_parts[0],
              'schema': relation_parts[1],
              'identifier': relation_parts[2]
            },
            'columns': desired_columns
          } %}
          {% set payload_literal =
            dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(action_payload)
          %}
          {% set call_sql %}
            CALL {{ procedure_relation }}(PARSE_JSON({{ payload_literal }}))
          {% endset %}
          {% set result_table = run_query(call_sql) %}
          {% set raw = result_table.columns[0].values()[0] %}
          {% set evolve_result = fromjson(raw) if raw is string else raw %}
          {% if not expect_success %}
            {% set error_message = evolve_result.get('error_message', '') | string %}
            {% if evolve_result.get('status') == 'success' %}
              {{ exceptions.raise_compiler_error(
                'expected evolve_schema to reject incompatible nested type change'
              ) }}
            {% endif %}
            {% if 'incompatible type change' not in error_message %}
              {{ exceptions.raise_compiler_error(
                'expected incompatible type change error, found ' ~ evolve_result
              ) }}
            {% endif %}
            {{ return('') }}
          {% endif %}
          {% if evolve_result.get('status') != 'success' %}
            {{ exceptions.raise_compiler_error(
              'evolve_schema failed: ' ~ evolve_result
            ) }}
          {% endif %}
          {% if evolve_result.get('altered_schema') != expect_altered %}
            {{ exceptions.raise_compiler_error(
              'expected altered_schema=' ~ expect_altered ~
              ', found ' ~ evolve_result.get('altered_schema')
            ) }}
          {% endif %}
          {% if expect_warning_substring is not none %}
            {% set warnings = evolve_result.get('warnings', []) | join(' | ') %}
            {% if expect_warning_substring not in warnings %}
              {{ exceptions.raise_compiler_error(
                'expected warning containing ' ~ expect_warning_substring ~
                ', found ' ~ warnings
              ) }}
            {% endif %}
          {% endif %}
          {% if expect_type_fragment is not none %}
            {% set describe = run_query('DESCRIBE TABLE ' ~ table_relation) %}
            {% set types = describe.columns[1].values() | list %}
            {% set joined = types | join(' || ') | upper %}
            {% if expect_type_fragment | upper not in joined %}
              {{ exceptions.raise_compiler_error(
                'expected type fragment ' ~ expect_type_fragment ~
                ' in ' ~ joined
              ) }}
            {% endif %}
          {% endif %}
        {% endmacro %}

        {% macro cleanup_nested_schema_evolution_tables(
          table_relations,
          procedure_relation,
          run_log_relation,
          handler_stage,
          drop_handler_stage,
          parquet_file_format,
          stage_fqn,
          stage_prefix
        ) %}
          {% for table_relation in table_relations %}
            {% call statement('drop_nested_table_' ~ loop.index, auto_begin=False) %}
              DROP ICEBERG TABLE IF EXISTS {{ table_relation }}
            {% endcall %}
          {% endfor %}
          {{ cleanup_s3_parquet_integration(
            [],
            procedure_relation,
            run_log_relation,
            handler_stage,
            drop_handler_stage,
            parquet_file_format,
            stage_fqn,
            stage_prefix
          ) }}
        {% endmacro %}

        {% macro cleanup_s3_parquet_integration(
          objects,
          procedure_relation,
          run_log_relation,
          handler_stage,
          drop_handler_stage,
          parquet_file_format,
          stage_fqn,
          stage_prefix
        ) %}
          {% for object in objects %}
            {% call statement('drop_view_' ~ loop.index, auto_begin=False) %}
              DROP VIEW IF EXISTS {{ object['view_relation'] }}
            {% endcall %}
            {% call statement('drop_table_' ~ loop.index, auto_begin=False) %}
              DROP ICEBERG TABLE IF EXISTS {{ object['internal_relation'] }}
            {% endcall %}
          {% endfor %}
          {% call statement('drop_procedure', auto_begin=False) %}
            DROP PROCEDURE IF EXISTS {{ procedure_relation }}(VARIANT)
          {% endcall %}
          {% call statement('drop_run_log', auto_begin=False) %}
            DROP TABLE IF EXISTS {{ run_log_relation }}
          {% endcall %}
          {% call statement('drop_file_format', auto_begin=False) %}
            DROP FILE FORMAT IF EXISTS {{ parquet_file_format }}
          {% endcall %}
          {% if drop_handler_stage %}
            {% call statement('drop_handler_stage', auto_begin=False) %}
              DROP STAGE IF EXISTS {{ handler_stage }}
            {% endcall %}
          {% endif %}
          {% call statement('remove_stage_files', auto_begin=False) %}
            REMOVE @{{ stage_fqn }}/{{ stage_prefix }}
          {% endcall %}
        {% endmacro %}
        """
    ).lstrip()


def _profile_yaml(*, database: str, schema: str) -> str:
    optional_lines = []
    password = os.environ.get("SNOWFLAKE_PASSWORD") or None
    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH") or None
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR") or None
    if authenticator is None and not password and not private_key_path:
        authenticator = "externalbrowser"

    for profile_name, value in (
        ("role", os.environ.get("SNOWFLAKE_ROLE") or None),
        ("warehouse", os.environ.get("SNOWFLAKE_WAREHOUSE") or None),
        ("authenticator", authenticator),
    ):
        if value:
            optional_lines.append(f"      {profile_name}: {value}")
    if password:
        optional_lines.append(
            "      password: \"{{ env_var('SNOWFLAKE_PASSWORD') }}\""
        )
    if private_key_path:
        optional_lines.append(
            "      private_key_path: \"{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}\""
        )

    return (
        textwrap.dedent(
            f"""
        iceberg_sync_integration:
          target: integration
          outputs:
            integration:
              type: snowflake
              account: {_required_env("SNOWFLAKE_ACCOUNT")}
              user: {_required_env("SNOWFLAKE_USER")}
              database: {database}
              schema: {schema}
              threads: 4
              client_session_keep_alive: False
        """
        ).lstrip()
        + "\n".join(optional_lines)
        + "\n"
    )


def _dbt_executable() -> str:
    return os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_DBT_EXECUTABLE") or "dbt"


def _dbt_env() -> dict[str, str]:
    env = dict(os.environ)
    env["DBT_PROJECT_DIR"] = env.get("DBT_PROJECT_DIR", "")
    return env


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"missing required environment variable: {name}")
    return value


def _quoted_relation(database: str, schema: str, identifier: str) -> str:
    return ".".join(f'"{part.upper()}"' for part in (database, schema, identifier))


def _unquoted_relation(database: str, schema: str, identifier: str) -> str:
    return f"{database}.{schema}.{identifier}"


def _jstr(value: str) -> str:
    return json.dumps(value)


def _jlist(values: list[str]) -> str:
    return "[" + ", ".join(_jstr(value) for value in values) + "]"


def _jinja_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return _jlist([str(item) for item in value])
    if value is None:
        return "none"
    return _jstr(str(value))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, *sys.argv[1:]]))
