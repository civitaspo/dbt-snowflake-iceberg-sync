from __future__ import annotations

import copy
from typing import Any

import pytest


@pytest.fixture
def base_payload() -> dict[str, Any]:
    return {
        "source_type": "bigquery",
        "materialization_strategy": "incremental",
        "incremental_strategy": "delete+copy",
        "incremental_predicate": None,
        "dbt_full_refresh": False,
        "partition_by": [],
        "cluster_by": [],
        "target_relation": {
            "database": "ANALYTICS",
            "schema": "PUBLIC",
            "identifier": "orders",
        },
        "internal_relation": {
            "database": "ANALYTICS",
            "schema": "PUBLIC",
            "identifier": "__orders",
        },
        "model": {
            "unique_id": "model.test.orders",
            "name": "orders",
            "sql": "",
            "invocation_id": "invocation-1",
        },
        "model_config": {},
        "deployment": {
            "procedure_database": "ANALYTICS",
            "procedure_schema": "UTIL",
            "procedure_name": "ICEBERG_SYNC",
            "run_log_table": {
                "database": "ANALYTICS",
                "schema": "UTIL",
                "identifier": "ICEBERG_SYNC_RUN_LOG",
            },
            "google_cloud_service_account_secret_alias": (
                "google_cloud_service_account_credentials_json"
            ),
            "google_cloud_auth_method": "service_account_credentials_json",
            "google_cloud_workload_identity_federation_secret_fqdn": None,
            "google_cloud_workload_identity_federation_audience": None,
            "google_cloud_service_account_impersonation": None,
        },
        "retry": {
            "max_attempts": 3,
            "initial_delay_seconds": 5,
            "max_delay_seconds": 60,
            "backoff_multiplier": 2.0,
            "jitter_seconds": 3,
        },
        "cleanup": {
            "created_table_on_failure": True,
        },
        "run_log": {
            "fail_on_error": False,
        },
        "bigquery": {
            "export_strategy": "extract",
            "project_id": "project",
            "dataset_id": "dataset",
            "table_id": "orders",
            "location": "US",
            "export_location": "@ANALYTICS.PUBLIC.EXPORT_STAGE/dbt",
            "export_compression": "ZSTD",
            "export_predicate_type": "auto",
            "full_refresh_predicates": [],
            "incremental_predicates": [],
            "staging_dataset_id": None,
            "staging_table_expiration_hours": 24,
            "staging_table_reuse": True,
            "force_rebuild_staging_table": False,
        },
        "iceberg_table": {
            "external_volume": "ICEBERG_EXTERNAL_VOLUME",
            "base_location": None,
            "target_file_size": "AUTO",
            "storage_serialization_policy": "COMPATIBLE",
            "data_retention_time_in_days": 7,
            "max_data_extension_time_in_days": None,
            "change_tracking": True,
            "copy_grants": False,
            "error_logging": False,
            "iceberg_version": 3,
            "enable_iceberg_merge_on_read": True,
            "enable_data_compaction": True,
        },
    }


@pytest.fixture
def s3_parquet_payload(base_payload) -> dict[str, Any]:
    payload = copy.deepcopy(base_payload)
    payload["source_type"] = "s3_parquet"
    payload["model"]["sql"] = ""
    payload["deployment"]["parquet_file_format"] = "ANALYTICS.UTIL.ICEBERG_SYNC_PARQUET_FILE_FORMAT"
    payload.pop("bigquery", None)
    payload["s3_parquet"] = {
        "location": "@ANALYTICS.PUBLIC.S3_EXPORT_STAGE/orders",
        "file_pattern": None,
        "full_refresh_paths": [""],
        "incremental_paths": [""],
        "skip_missing_location": False,
        "infer_schema_max_file_count": 16,
    }
    return payload


@pytest.fixture
def payload_factory(base_payload: dict[str, Any]):
    def factory(**updates: Any) -> dict[str, Any]:
        payload = copy.deepcopy(base_payload)
        for path, value in updates.items():
            target = payload
            parts = path.split("__")
            for part in parts[:-1]:
                target = target[part]
            target[parts[-1]] = value
        return payload

    return factory


@pytest.fixture
def s3_payload_factory(s3_parquet_payload: dict[str, Any]):
    def factory(**updates: Any) -> dict[str, Any]:
        payload = copy.deepcopy(s3_parquet_payload)
        for path, value in updates.items():
            target = payload
            parts = path.split("__")
            for part in parts[:-1]:
                if part not in target or not isinstance(target[part], dict):
                    target[part] = {}
                target = target[part]
            target[parts[-1]] = value
        return payload

    return factory
