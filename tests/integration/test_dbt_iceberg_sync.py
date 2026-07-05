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


def test_dbt_extract_skips_missing_table(tmp_path: Path):
    context = _integration_context(tmp_path, "extract_skip_missing")
    model_name = f"iceberg_sync_extract_skip_missing_{context.run_id}"
    missing_table_id = f"missing_{context.run_id}"
    export_prefix = _export_prefix(context, model_name)
    models = {
        model_name: _extract_model_sql(
            context,
            model_name=model_name,
            table_id=missing_table_id,
            export_predicate_type="none",
            base_location=export_prefix,
            export_prefix=export_prefix,
            extra_config={"bigquery_extract_skip_missing_tables": True},
        )
    }

    _write_project(context, models)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run", "--select", model_name)
        _assert_skipped_model(context, model_name)
    finally:
        _cleanup(context, [model_name])


def test_dbt_extract_datetime(tmp_path: Path):
    context = _integration_context(tmp_path, "datetime")
    model_name = f"iceberg_sync_datetime_{context.run_id}"
    export_prefix = _export_prefix(context, model_name)
    models = {
        model_name: _extract_model_sql(
            context,
            model_name=model_name,
            table_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_TABLE_ID"),
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
                    expected_rows=_required_int_env(
                        "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_EXPECTED_ROWS"
                    ),
                    expected_modes=["full_refresh"],
                    expected_source_job_reference_counts=[1],
                    expected_column_types=[
                        {
                            "name": "occurred_datetime",
                            "type": "TIMESTAMP_NTZ(6)",
                        }
                    ],
                    expected_view_values={
                        "column": "occurred_datetime",
                        "values": sorted(
                            _required_env_list(
                                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_EXPECTED_VALUES"
                            )
                        ),
                    },
                )
            ],
        )
    finally:
        _cleanup(context, [model_name])


def test_dbt_extract_modes(tmp_path: Path):
    context = _integration_context(tmp_path, "extract_modes")
    cases = [
        {
            "suffix": "non_partitioned_auto",
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
            "suffix": "non_partitioned_none",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "none",
            "full_refresh_predicates": [],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "time_partitioned_auto",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [
                _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_DECORATOR")
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "time_partitioned_decorator",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "partition_decorator",
            "full_refresh_predicates": [
                _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_DECORATOR")
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "range_partitioned_auto",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [
                _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_DECORATOR")
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "range_partitioned_decorator",
            "table_id": _required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITIONED_TABLE_ID"
            ),
            "export_predicate_type": "partition_decorator",
            "full_refresh_predicates": [
                _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_DECORATOR")
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "sharded_auto_all",
            "table_id": _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_TABLE_ID"),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_ALL_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "sharded_none_all",
            "table_id": _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_TABLE_ID"),
            "export_predicate_type": "none",
            "full_refresh_predicates": [],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_ALL_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "sharded_auto_suffix",
            "table_id": _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_TABLE_ID"),
            "export_predicate_type": "auto",
            "full_refresh_predicates": [
                _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_SUFFIX")
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_SUFFIX_EXPECTED_ROWS"
            ),
        },
        {
            "suffix": "sharded_table_suffix",
            "table_id": _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_TABLE_ID"),
            "export_predicate_type": "table_suffix",
            "full_refresh_predicates": [
                _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_SUFFIX")
            ],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_SUFFIX_EXPECTED_ROWS"
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


def test_dbt_extract_compression_modes(tmp_path: Path):
    context = _integration_context(tmp_path, "extract_compression")
    table_id = _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID")
    expected_rows = _required_int_env(
        "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_EXPECTED_ROWS"
    )
    model_sql: dict[str, str] = {}
    assertions: list[dict[str, Any]] = []
    model_names: list[str] = []
    for codec in ("NONE", "SNAPPY", "GZIP", "ZSTD"):
        model_name = f"iceberg_sync_compression_{codec.lower()}_{context.run_id}"
        export_prefix = _export_prefix(context, model_name)
        model_names.append(model_name)
        model_sql[model_name] = _extract_model_sql(
            context,
            model_name=model_name,
            table_id=table_id,
            export_predicate_type="none",
            base_location=export_prefix,
            export_prefix=export_prefix,
            extra_config={"bigquery_export_compression": codec},
        )
        assertions.append(
            _assertion(
                context,
                model_name,
                expected_rows=expected_rows,
                expected_modes=["full_refresh"],
                expected_source_job_reference_counts=[1],
            )
        )

    _write_project(context, model_sql)
    try:
        _run_dbt(context, "deps")
        _run_dbt(context, "run", "--select", *model_names)
        _assert_models(context, assertions)
    finally:
        _cleanup(context, model_names)


def test_dbt_select_query_export(tmp_path: Path):
    context = _integration_context(tmp_path, "select")
    cases = [
        {
            "suffix": "auto_all",
            "predicate_type": "auto",
            "predicates": [],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_ALL_EXPECTED_ROWS"
            ),
            "expected_job_counts": [2],
            "staging_table_reuse": False,
            "runs": 1,
        },
        {
            "suffix": "none_all",
            "predicate_type": "none",
            "predicates": [],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_ALL_EXPECTED_ROWS"
            ),
            "expected_job_counts": [2],
            "staging_table_reuse": False,
            "runs": 1,
        },
        {
            "suffix": "auto_where",
            "predicate_type": "auto",
            "predicates": [_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE")],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS"
            ),
            "expected_job_counts": [2],
            "staging_table_reuse": False,
            "runs": 1,
        },
        {
            "suffix": "where",
            "predicate_type": "where",
            "predicates": [_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE")],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS"
            ),
            "expected_job_counts": [2],
            "staging_table_reuse": False,
            "runs": 1,
        },
        {
            "suffix": "reuse",
            "predicate_type": "where",
            "predicates": [_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE")],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS"
            ),
            "expected_job_counts": [2, 1],
            "staging_table_reuse": True,
            "runs": 2,
        },
        {
            "suffix": "force_rebuild",
            "predicate_type": "where",
            "predicates": [_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE")],
            "expected_rows": _required_int_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS"
            ),
            "expected_job_counts": [2, 2],
            "staging_table_reuse": True,
            "force_rebuild_staging_table": True,
            "runs": 2,
        },
    ]
    model_sql: dict[str, str] = {}
    assertions: list[dict[str, Any]] = []
    model_names: list[str] = []
    for case in cases:
        model_name = f"iceberg_sync_select_{case['suffix']}_{context.run_id}"
        export_prefix = _export_prefix(context, model_name)
        model_names.append(model_name)
        model_sql[model_name] = _select_model_sql(
            context,
            model_name=model_name,
            model_sql=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_SQL"),
            predicates=case["predicates"],
            predicate_type=str(case["predicate_type"]),
            table_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_TABLE_ID"),
            staging_dataset_id=_required_env(
                "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
            ),
            base_location=export_prefix,
            export_prefix=export_prefix,
            staging_table_reuse=bool(case["staging_table_reuse"]),
            force_rebuild_staging_table=bool(case.get("force_rebuild_staging_table", False)),
        )
        assertions.append(
            _assertion(
                context,
                model_name,
                expected_rows=int(case["expected_rows"]),
                expected_modes=["full_refresh"] * int(case["runs"]),
                expected_source_job_reference_counts=list(case["expected_job_counts"]),
                require_staging_table=True,
            )
        )

    _write_project(context, model_sql)
    try:
        _run_dbt(context, "deps")
        for case, model_name in zip(cases, model_names, strict=True):
            for _ in range(int(case["runs"])):
                _run_dbt(context, "run", "--select", model_name)
        _assert_models(context, assertions)
    finally:
        _cleanup(context, model_names)


def test_dbt_incremental_delete_copy(tmp_path: Path):
    context = _integration_context(tmp_path, "incremental")
    model_name = f"iceberg_sync_incremental_{context.run_id}"
    export_prefix = _export_prefix(context, model_name)
    model_sql = {
        model_name: _extract_model_sql(
            context,
            model_name=model_name,
            table_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_TABLE_ID"),
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
            incremental_predicate=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_PREDICATE"),
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


def test_dbt_invalid_parameter_combinations(tmp_path: Path):
    context = _integration_context(tmp_path, "invalid")
    sharded_table_id = _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_TABLE_ID")
    cases = [
        {
            "suffix": "extract_where",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_extract_where_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="where",
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "extract export strategy does not support where",
        },
        {
            "suffix": "extract_sql",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_extract_sql_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="auto",
                base_location="unused",
                export_prefix="unused",
                model_sql="select 1 as ignored",
            ),
            "message": "model SQL is only supported",
        },
        {
            "suffix": "select_table_suffix",
            "sql": _select_model_sql(
                context,
                model_name=f"invalid_select_table_suffix_{context.run_id}",
                model_sql="select 1 as id",
                predicates=["20240111"],
                predicate_type="table_suffix",
                table_id="select_query_source",
                staging_dataset_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "select export strategy allows only",
        },
        {
            "suffix": "select_partition_decorator",
            "sql": _select_model_sql(
                context,
                model_name=f"invalid_select_partition_decorator_{context.run_id}",
                model_sql="select 1 as id",
                predicates=["20240111"],
                predicate_type="partition_decorator",
                table_id="select_query_source",
                staging_dataset_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "select export strategy allows only",
        },
        {
            "suffix": "select_missing_staging",
            "sql": _select_model_sql(
                context,
                model_name=f"invalid_select_missing_staging_{context.run_id}",
                model_sql="select 1 as id",
                predicates=[],
                predicate_type="none",
                table_id="select_query_source",
                staging_dataset_id=None,
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "bigquery_staging_dataset_id",
        },
        {
            "suffix": "select_missing_sql",
            "sql": _select_model_sql(
                context,
                model_name=f"invalid_select_missing_sql_{context.run_id}",
                model_sql="",
                predicates=[],
                predicate_type="none",
                table_id="select_query_source",
                staging_dataset_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "model SQL is required",
        },
        {
            "suffix": "incremental_bq_only",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_incremental_bq_only_{context.run_id}",
                table_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_TABLE_ID"),
                export_predicate_type="auto",
                materialization_strategy="incremental",
                full_refresh_predicates=_required_env_list(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_FULL_REFRESH_PREDICATES"
                ),
                incremental_predicates=_required_env_list(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_PREDICATES"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "both present or both absent",
        },
        {
            "suffix": "incremental_snowflake_only",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_incremental_snowflake_only_{context.run_id}",
                table_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_TABLE_ID"),
                export_predicate_type="auto",
                materialization_strategy="incremental",
                full_refresh_predicates=_required_env_list(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_FULL_REFRESH_PREDICATES"
                ),
                incremental_predicate=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_PREDICATE"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "both present or both absent",
        },
        {
            "suffix": "forbidden_secret_config",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_forbidden_secret_config_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="auto",
                base_location="unused",
                export_prefix="unused",
                extra_config={"google_cloud_service_account_secret_fqdn": "DB.SCHEMA.SECRET"},
            ),
            "message": "credential material",
        },
        {
            "suffix": "user_stage",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_user_stage_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="auto",
                base_location="unused",
                export_prefix="unused",
                export_location="@~/exports",
            ),
            "message": "named Snowflake stage",
        },
        {
            "suffix": "none_with_predicates",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_none_with_predicates_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="none",
                full_refresh_predicates=[
                    _required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_DECORATOR")
                ],
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "does not accept predicates",
        },
        {
            "suffix": "table_suffix_concrete",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_table_suffix_concrete_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="table_suffix",
                full_refresh_predicates=["20240111"],
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "ending with",
        },
        {
            "suffix": "table_suffix_empty",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_table_suffix_empty_{context.run_id}",
                table_id=sharded_table_id,
                export_predicate_type="table_suffix",
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "at least one predicate",
        },
        {
            "suffix": "partition_decorator_non_partitioned",
            "sql": _extract_model_sql(
                context,
                model_name=f"invalid_partition_decorator_non_partitioned_{context.run_id}",
                table_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID"
                ),
                export_predicate_type="partition_decorator",
                full_refresh_predicates=["20240111"],
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "native partitioned table",
        },
        {
            "suffix": "select_where_empty",
            "sql": _select_model_sql(
                context,
                model_name=f"invalid_select_where_empty_{context.run_id}",
                model_sql="select 1 as id",
                predicates=[],
                predicate_type="where",
                table_id="select_query_source",
                staging_dataset_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "requires at least one predicate",
        },
        {
            "suffix": "select_none_with_predicates",
            "sql": _select_model_sql(
                context,
                model_name=f"invalid_select_none_with_predicates_{context.run_id}",
                model_sql="select 1 as id",
                predicates=["id = 1"],
                predicate_type="none",
                table_id="select_query_source",
                staging_dataset_id=_required_env(
                    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID"
                ),
                base_location="unused",
                export_prefix="unused",
            ),
            "message": "does not accept predicates",
        },
    ]
    models = {f"iceberg_sync_{case['suffix']}_{context.run_id}": str(case["sql"]) for case in cases}
    _write_project(context, models)
    try:
        _run_dbt(context, "deps")
        for case, model_name in zip(cases, models, strict=True):
            _run_dbt_expect_failure(
                context,
                "run",
                "--select",
                model_name,
                expected_message=str(case["message"]),
            )
    finally:
        _cleanup(context, list(models))


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
        bigquery_project_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID"),
        bigquery_dataset_id=_required_env("DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID"),
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
                handler_local_path: {json.dumps(str(context.package_path / "procedure"))}
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
    dataset_id: str | None = None,
    export_location: str | None = None,
    materialization_strategy: str = "full_refresh",
    full_refresh_predicates: list[str] | None = None,
    incremental_predicates: list[str] | None = None,
    incremental_predicate: str | None = None,
    model_sql: str = "",
    extra_config: dict[str, Any] | None = None,
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
                'incremental_strategy': 'delete+copy',
                'incremental_predicate': iceberg_sync_incremental_predicate | trim,
            """
        )
    extra_config_sql = ""
    if extra_config:
        extra_config_sql = "".join(
            f"              '{key}': {_jinja_value(value)},\n"
            for key, value in extra_config.items()
        )
    export_location = export_location or f"@{context.export_stage}/{export_prefix}"
    return textwrap.dedent(
        f"""
        {incremental_setup}
        {{{{
          config(
            materialized='iceberg_sync',
            meta={{
              'iceberg_sync': {{
                'source_type': 'bigquery',
                'materialization_strategy': {_jstr(materialization_strategy)},
        {textwrap.indent(incremental_config, "    ").rstrip()}
                'bigquery_export_strategy': 'extract',
                'google_cloud_project_id': {_jstr(context.bigquery_project_id)},
                'bigquery_dataset_id': {_jstr(dataset_id or context.bigquery_dataset_id)},
                'bigquery_table_id': {_jstr(table_id)},
                'bigquery_location': {_jstr(context.bigquery_location)},
                'bigquery_export_location': {_jstr(export_location)},
                'bigquery_export_predicate_type': {_jstr(export_predicate_type)},
                'bigquery_export_full_refresh_predicates': {_jlist(full_refresh_predicates or [])},
                'bigquery_export_incremental_predicates': {_jlist(incremental_predicates or [])},
                'iceberg_table_external_volume': {_jstr(context.external_volume)},
                'iceberg_table_base_location': {_jstr(base_location)},
{extra_config_sql.rstrip()}
              }}
            }}
          )
        }}}}
        {model_sql}
        """
    ).lstrip()


def _select_model_sql(
    context: IntegrationContext,
    *,
    model_name: str,
    model_sql: str,
    predicates: list[str],
    predicate_type: str,
    table_id: str,
    staging_dataset_id: str | None,
    base_location: str,
    export_prefix: str,
    staging_table_reuse: bool = False,
    force_rebuild_staging_table: bool = False,
) -> str:
    export_location = f"@{context.export_stage}/{export_prefix}"
    return textwrap.dedent(
        f"""
        {{{{
          config(
            materialized='iceberg_sync',
            meta={{
              'iceberg_sync': {{
                'source_type': 'bigquery',
                'materialization_strategy': 'full_refresh',
                'bigquery_export_strategy': 'select',
                'google_cloud_project_id': {_jstr(context.bigquery_project_id)},
                'bigquery_dataset_id': {_jstr(context.bigquery_dataset_id)},
                'bigquery_table_id': {_jstr(table_id)},
                'bigquery_location': {_jstr(context.bigquery_location)},
                'bigquery_export_location': {_jstr(export_location)},
                'bigquery_export_predicate_type': {_jstr(predicate_type)},
                'bigquery_export_full_refresh_predicates': {_jlist(predicates)},
                'bigquery_staging_dataset_id': {_jinja_value(staging_dataset_id)},
                'bigquery_staging_table_reuse': {_jbool(staging_table_reuse)},
                'force_rebuild_staging_table': {_jbool(force_rebuild_staging_table)},
                'iceberg_table_external_volume': {_jstr(context.external_volume)},
                'iceberg_table_base_location': {_jstr(base_location)}
              }}
            }}
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
    expected_source_job_reference_counts: list[int] | None = None,
    expected_column_types: list[dict[str, str]] | None = None,
    expected_view_values: dict[str, Any] | None = None,
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
        "expected_source_job_reference_counts": expected_source_job_reference_counts,
        "expected_column_types": expected_column_types,
        "expected_view_values": expected_view_values,
    }


def _assert_models(context: IntegrationContext, models: list[dict[str, Any]]) -> None:
    _run_dbt(
        context,
        "run-operation",
        "assert_iceberg_sync_integration",
        "--args",
        json.dumps({"models": models, "run_log_relation": context.run_log_relation}),
    )


def _assert_skipped_model(context: IntegrationContext, model_name: str) -> None:
    _run_dbt(
        context,
        "run-operation",
        "assert_iceberg_sync_skipped",
        "--args",
        json.dumps(
            {
                "model": {
                    "database": context.snowflake_database,
                    "schema": context.snowflake_schema,
                    "identifier": model_name,
                    "target_view": _quoted_relation(
                        context.snowflake_database,
                        context.snowflake_schema,
                        model_name,
                    ),
                },
                "run_log_relation": context.run_log_relation,
            }
        ),
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

            {% if model.get('expected_source_job_reference_counts') is not none %}
              {% set job_count_sql %}
                select coalesce(array_size(source_job_references), 0) as job_count
                from {{ run_log_relation }}
                where target_view = '{{ model['target_view'] | replace("'", "''") }}'
                  and status = 'success'
                order by started_at
              {% endset %}
              {% set job_count_rows = run_query(job_count_sql) %}
              {% set actual_job_counts = [] %}
              {% for row in job_count_rows.rows %}
                {% do actual_job_counts.append(row[0] | int) %}
              {% endfor %}
              {% if actual_job_counts != model.get('expected_source_job_reference_counts') %}
                {% set message %}
                  {{ model['view_relation'] }} expected source job reference counts
                  {{ model.get('expected_source_job_reference_counts') }},
                  got {{ actual_job_counts }}
                {% endset %}
                {{ exceptions.raise_compiler_error(message) }}
              {% endif %}
            {% endif %}

            {% if model.get('expected_column_types') is not none %}
              {% set describe_sql %}
                describe table {{ model['internal_relation'] }}
              {% endset %}
              {% set describe_rows = run_query(describe_sql) %}
              {% set actual_column_types = {} %}
              {% for row in describe_rows.rows %}
                {% do actual_column_types.update({
                  (row[0] | string | lower): (row[1] | string | upper)
                }) %}
              {% endfor %}
              {% for expected in model.get('expected_column_types') %}
                {% set actual_type = actual_column_types.get(expected['name'] | lower) %}
                {% if actual_type != (expected['type'] | upper) %}
                  {% set message %}
                    {{ model['internal_relation'] }} expected column
                    {{ expected['name'] }} type {{ expected['type'] }},
                    got {{ actual_type }}
                  {% endset %}
                  {{ exceptions.raise_compiler_error(message) }}
                {% endif %}
              {% endfor %}
            {% endif %}

            {% if model.get('expected_view_values') is not none %}
              {% set value_config = model.get('expected_view_values') %}
              {% set value_sql %}
                select to_varchar(
                  "{{ value_config['column'] | upper }}",
                  'YYYY-MM-DD HH24:MI:SS.FF6'
                ) as value
                from {{ model['view_relation'] }}
                order by value
              {% endset %}
              {% set value_rows = run_query(value_sql) %}
              {% set actual_values = [] %}
              {% for row in value_rows.rows %}
                {% do actual_values.append(row[0]) %}
              {% endfor %}
              {% if actual_values != value_config.get('values') %}
                {% set message %}
                  {{ model['view_relation'] }} expected
                  {{ value_config.get('values') }} for {{ value_config['column'] }},
                  got {{ actual_values }}
                {% endset %}
                {{ exceptions.raise_compiler_error(message) }}
              {% endif %}
            {% endif %}
            {% endfor %}
        {% endmacro %}

        {% macro assert_iceberg_sync_skipped(model, run_log_relation) %}
          {% set view_relation = adapter.get_relation(
            database=model['database'],
            schema=model['schema'],
            identifier=model['identifier']
          ) %}
          {% if view_relation is not none %}
            {{ exceptions.raise_compiler_error(model['identifier'] ~ ' view was created') }}
          {% endif %}

          {% set internal_relation = adapter.get_relation(
            database=model['database'],
            schema=model['schema'],
            identifier='__' ~ model['identifier']
          ) %}
          {% if internal_relation is not none %}
            {% set message = model['identifier'] ~ ' internal table was created' %}
            {{ exceptions.raise_compiler_error(message) }}
          {% endif %}

          {% set skipped_sql %}
            select
              count_if(status = 'skipped') as skipped_count,
              count_if(status = 'success') as success_count
            from {{ run_log_relation }}
            where target_view = '{{ model['target_view'] | replace("'", "''") }}'
          {% endset %}
          {% set skipped_rows = run_query(skipped_sql) %}
          {% set skipped_count = skipped_rows.rows[0][0] | int %}
          {% set success_count = skipped_rows.rows[0][1] | int %}
          {% if skipped_count != 1 or success_count != 0 %}
            {% set message %}
              {{ model['identifier'] }} expected one skipped run-log row and no success rows,
              got skipped={{ skipped_count }}, success={{ success_count }}
            {% endset %}
            {{ exceptions.raise_compiler_error(message) }}
          {% endif %}
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


def _run_dbt_expect_failure(
    context: IntegrationContext,
    *args: str,
    expected_message: str,
) -> None:
    result = subprocess.run(
        [
            _dbt_executable(),
            *args,
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
    if result.returncode == 0:
        raise AssertionError(f"dbt command unexpectedly succeeded: {' '.join(args)}")
    if expected_message not in result.stdout:
        raise AssertionError(
            f"dbt command failed without expected message {expected_message!r}.\n"
            f"Output:\n{result.stdout}"
        )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        if os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION") == "1":
            pytest.fail(f"{name} is required when DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1")
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
    if executable := os.environ.get("DBT_SNOWFLAKE_ICEBERG_SYNC_DBT_EXECUTABLE"):
        return executable
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

    return (
        textwrap.dedent(
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
        ).lstrip()
        + "\n".join(optional_lines)
        + "\n"
    )


def _export_prefix(context: IntegrationContext, model_name: str) -> str:
    return f"dbt_iceberg_sync_integration/{context.run_id}/{model_name}"


def _quoted_relation(database: str, schema: str, identifier: str) -> str:
    return ".".join(f'"{part.upper()}"' for part in (database, schema, identifier))


def _unquoted_relation(database: str, schema: str, identifier: str) -> str:
    return ".".join(part.upper() for part in (database, schema, identifier))


def _jstr(value: str) -> str:
    return json.dumps(value)


def _jlist(value: list[str]) -> str:
    return json.dumps(value)


def _jbool(value: bool) -> str:
    return "true" if value else "false"


def _jinja_value(value: Any) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        return _jbool(value)
    if isinstance(value, list):
        return _jlist(value)
    return json.dumps(value)
