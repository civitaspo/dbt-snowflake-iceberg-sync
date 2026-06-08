"""Config parsing and validation for the Iceberg sync procedure."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import ConfigError
from .utils import normalize_snowflake_object_identifier

SUPPORTED_SOURCE_TYPES = {"bigquery"}
MATERIALIZATION_STRATEGIES = {"full_refresh", "incremental"}
BIGQUERY_EXPORT_STRATEGIES = {"extract", "select"}
PREDICATE_TYPES = {"auto", "none", "partition_decorator", "table_suffix", "where"}
INCREMENTAL_STRATEGIES = {"delete+copy"}
STORAGE_SERIALIZATION_POLICIES = {"COMPATIBLE", "OPTIMIZED"}
FORBIDDEN_MODEL_CONFIG_KEYS = {
    "credentials",
    "credential",
    "password",
    "private_key",
    "service_account",
    "service_account_json",
    "google_cloud_service_account_json",
    "google_cloud_service_account_secret_fqdn",
    "google_cloud_service_account_secret_alias",
    "google_application_credentials",
}


@dataclass(frozen=True)
class RelationConfig:
    database: str
    schema: str
    identifier: str

    @property
    def fqn(self) -> str:
        return f"{self.database}.{self.schema}.{self.identifier}"


@dataclass(frozen=True)
class ModelConfig:
    unique_id: str
    name: str
    sql: str
    invocation_id: str | None = None


@dataclass(frozen=True)
class DeploymentConfig:
    procedure_database: str | None = None
    procedure_schema: str | None = None
    procedure_name: str | None = None
    run_log_table: RelationConfig | None = None
    google_cloud_service_account_secret_alias: str | None = None


@dataclass(frozen=True)
class BigQueryConfig:
    export_strategy: str
    project_id: str
    dataset_id: str
    table_id: str
    location: str
    export_location: str
    export_predicate_type: str = "auto"
    full_refresh_predicates: tuple[str, ...] = field(default_factory=tuple)
    incremental_predicates: tuple[str, ...] = field(default_factory=tuple)
    staging_dataset_id: str | None = None
    staging_table_expiration_hours: int = 24
    staging_table_reuse: bool = True
    force_rebuild_staging_table: bool = False


@dataclass(frozen=True)
class IcebergTableConfig:
    external_volume: str
    base_location: str | None = None
    target_file_size: str = "AUTO"
    storage_serialization_policy: str = "COMPATIBLE"
    data_retention_time_in_days: int = 7
    max_data_extension_time_in_days: int | None = None
    change_tracking: bool = True
    copy_grants: bool = False
    error_logging: bool = False
    iceberg_version: int = 3
    enable_iceberg_merge_on_read: bool = True
    enable_data_compaction: bool = True


@dataclass(frozen=True)
class IcebergSyncConfig:
    source_type: str
    materialization_strategy: str
    incremental_strategy: str
    incremental_predicate: str | None
    target_relation: RelationConfig
    internal_relation: RelationConfig
    model: ModelConfig
    deployment: DeploymentConfig
    bigquery: BigQueryConfig
    iceberg_table: IcebergTableConfig
    partition_by: tuple[str, ...] = field(default_factory=tuple)
    cluster_by: tuple[str, ...] = field(default_factory=tuple)
    dbt_full_refresh: bool = False

    def predicates_for_mode(self, effective_mode: str) -> tuple[str, ...]:
        if effective_mode == "full_refresh":
            return self.bigquery.full_refresh_predicates
        return self.bigquery.incremental_predicates


def parse_config(payload: dict[str, Any]) -> IcebergSyncConfig:
    payload = dict(payload or {})
    _reject_forbidden_model_config(payload.get("model_config", {}))

    target = _relation(payload.get("target_relation"), "target_relation")
    internal_payload = payload.get("internal_relation") or {
        "database": target.database,
        "schema": target.schema,
        "identifier": f"__{target.identifier}",
    }
    internal = _relation(internal_payload, "internal_relation")

    model_payload = payload.get("model", {})
    model = ModelConfig(
        unique_id=_required(model_payload, "unique_id", "model.unique_id"),
        name=_required(model_payload, "name", "model.name"),
        sql=str(model_payload.get("sql") or ""),
        invocation_id=model_payload.get("invocation_id"),
    )

    deployment_payload = payload.get("deployment", {})
    deployment = DeploymentConfig(
        procedure_database=_optional_object_identifier(
            deployment_payload.get("procedure_database")
        ),
        procedure_schema=_optional_object_identifier(deployment_payload.get("procedure_schema")),
        procedure_name=_optional_object_identifier(deployment_payload.get("procedure_name")),
        run_log_table=_optional_relation(deployment_payload.get("run_log_table"), "run_log_table"),
        google_cloud_service_account_secret_alias=deployment_payload.get(
            "google_cloud_service_account_secret_alias"
        ),
    )

    bq_payload = payload.get("bigquery", {})
    bigquery = BigQueryConfig(
        export_strategy=_defaulted(bq_payload, "export_strategy", "extract"),
        project_id=_required(bq_payload, "project_id", "bigquery.project_id"),
        dataset_id=_required(bq_payload, "dataset_id", "bigquery.dataset_id"),
        table_id=_required(bq_payload, "table_id", "bigquery.table_id"),
        location=_required(bq_payload, "location", "bigquery.location"),
        export_location=_required(bq_payload, "export_location", "bigquery.export_location"),
        export_predicate_type=_defaulted(bq_payload, "export_predicate_type", "auto"),
        full_refresh_predicates=tuple(bq_payload.get("full_refresh_predicates") or ()),
        incremental_predicates=tuple(bq_payload.get("incremental_predicates") or ()),
        staging_dataset_id=bq_payload.get("staging_dataset_id"),
        staging_table_expiration_hours=int(bq_payload.get("staging_table_expiration_hours", 24)),
        staging_table_reuse=_coerce_bool(bq_payload.get("staging_table_reuse"), True),
        force_rebuild_staging_table=_coerce_bool(
            bq_payload.get("force_rebuild_staging_table"),
            False,
        ),
    )

    iceberg_payload = payload.get("iceberg_table", {})
    iceberg_table = IcebergTableConfig(
        external_volume=_required(
            iceberg_payload, "external_volume", "iceberg_table.external_volume"
        ),
        base_location=iceberg_payload.get("base_location"),
        target_file_size=_defaulted(iceberg_payload, "target_file_size", "AUTO"),
        storage_serialization_policy=_defaulted(
            iceberg_payload, "storage_serialization_policy", "COMPATIBLE"
        ),
        data_retention_time_in_days=int(iceberg_payload.get("data_retention_time_in_days", 7)),
        max_data_extension_time_in_days=_optional_int(
            iceberg_payload.get("max_data_extension_time_in_days")
        ),
        change_tracking=_coerce_bool(iceberg_payload.get("change_tracking"), True),
        copy_grants=_coerce_bool(iceberg_payload.get("copy_grants"), False),
        error_logging=_coerce_bool(iceberg_payload.get("error_logging"), False),
        iceberg_version=int(iceberg_payload.get("iceberg_version", 3)),
        enable_iceberg_merge_on_read=_coerce_bool(
            iceberg_payload.get("enable_iceberg_merge_on_read"),
            True,
        ),
        enable_data_compaction=_coerce_bool(
            iceberg_payload.get("enable_data_compaction"),
            True,
        ),
    )

    config = IcebergSyncConfig(
        source_type=_defaulted(payload, "source_type", "bigquery"),
        materialization_strategy=_defaulted(payload, "materialization_strategy", "incremental"),
        incremental_strategy=_defaulted(payload, "incremental_strategy", "delete+copy"),
        incremental_predicate=payload.get("incremental_predicate"),
        target_relation=target,
        internal_relation=internal,
        model=model,
        deployment=deployment,
        bigquery=bigquery,
        iceberg_table=iceberg_table,
        partition_by=tuple(payload.get("partition_by") or ()),
        cluster_by=tuple(payload.get("cluster_by") or ()),
        dbt_full_refresh=_coerce_bool(payload.get("dbt_full_refresh"), False),
    )
    validate_config(config)
    return config


def validate_config(config: IcebergSyncConfig) -> None:
    if config.source_type not in SUPPORTED_SOURCE_TYPES:
        raise ConfigError("source_type must be 'bigquery'")
    if config.materialization_strategy not in MATERIALIZATION_STRATEGIES:
        raise ConfigError("materialization_strategy must be 'full_refresh' or 'incremental'")
    if config.incremental_strategy not in INCREMENTAL_STRATEGIES:
        raise ConfigError("incremental_strategy must be 'delete+copy'")
    if config.bigquery.export_strategy not in BIGQUERY_EXPORT_STRATEGIES:
        raise ConfigError("bigquery_export_strategy must be 'extract' or 'select'")
    if (
        config.iceberg_table.storage_serialization_policy
        not in STORAGE_SERIALIZATION_POLICIES
    ):
        raise ConfigError(
            "iceberg_table_storage_serialization_policy must be COMPATIBLE or OPTIMIZED"
        )
    if config.iceberg_table.iceberg_version not in {2, 3}:
        raise ConfigError("iceberg_table_iceberg_version must be 2 or 3")
    if config.iceberg_table.iceberg_version == 3 and not config.iceberg_table.change_tracking:
        raise ConfigError("iceberg_table_change_tracking must be true for Iceberg V3 tables")
    if config.iceberg_table.error_logging:
        raise ConfigError("iceberg_table_error_logging is not supported for Iceberg COPY INTO")
    if config.bigquery.export_predicate_type not in PREDICATE_TYPES:
        raise ConfigError("bigquery_export_predicate_type is invalid")
    if config.partition_by:
        raise ConfigError("partition_by is not supported by iceberg_sync in the first scope")
    if config.cluster_by:
        raise ConfigError("cluster_by is not supported by iceberg_sync in the first scope")
    if config.bigquery.export_strategy == "select" and not config.bigquery.staging_dataset_id:
        raise ConfigError("bigquery_staging_dataset_id is required for select export strategy")
    if config.bigquery.export_strategy == "select" and not config.model.sql.strip():
        raise ConfigError("model SQL is required for bigquery_export_strategy='select'")
    if (
        config.bigquery.export_strategy == "select"
        and config.bigquery.export_predicate_type
        not in {
            "auto",
            "none",
            "where",
        }
    ):
        raise ConfigError("select export strategy allows only auto, none, or where predicates")
    if (
        config.bigquery.export_strategy == "extract"
        and config.bigquery.export_predicate_type == "where"
    ):
        raise ConfigError("extract export strategy does not support where predicates")
    if config.bigquery.export_strategy == "extract" and config.model.sql.strip():
        # The model SQL is harmless, but it often means the author intended select mode.
        # Keep this as a validation error to avoid silently ignoring model logic.
        raise ConfigError("model SQL is only supported with bigquery_export_strategy='select'")
    has_incremental_bq_predicates = bool(config.bigquery.incremental_predicates)
    has_incremental_snowflake_predicate = bool(config.incremental_predicate)
    if has_incremental_bq_predicates != has_incremental_snowflake_predicate:
        raise ConfigError(
            "incremental BigQuery predicates and incremental_predicate must be both present "
            "or both absent"
        )
    if not config.bigquery.export_location.startswith("@"):
        raise ConfigError("bigquery_export_location must be a named Snowflake stage location")
    if config.bigquery.export_location.startswith(("@~", "@%")):
        raise ConfigError(
            "bigquery_export_location must be a named Snowflake stage, not a user or table stage"
        )


def _relation(value: Any, field_name: str) -> RelationConfig:
    if not isinstance(value, dict):
        raise ConfigError(f"{field_name} must be an object")
    return RelationConfig(
        database=normalize_snowflake_object_identifier(
            _required(value, "database", f"{field_name}.database")
        ),
        schema=normalize_snowflake_object_identifier(
            _required(value, "schema", f"{field_name}.schema")
        ),
        identifier=normalize_snowflake_object_identifier(
            _required(value, "identifier", f"{field_name}.identifier")
        ),
    )


def _optional_relation(value: Any, field_name: str) -> RelationConfig | None:
    if value in (None, ""):
        return None
    return _relation(value, field_name)


def _optional_object_identifier(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return normalize_snowflake_object_identifier(str(value))


def _required(value: dict[str, Any], key: str, field_name: str) -> str:
    result = value.get(key)
    if result is None or result == "":
        raise ConfigError(f"{field_name} is required")
    return str(result)


def _defaulted(value: dict[str, Any], key: str, default: str) -> str:
    result = value.get(key, default)
    return str(result if result is not None else default)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _reject_forbidden_model_config(model_config: Any) -> None:
    if not isinstance(model_config, dict):
        return
    lowered = {str(key).lower() for key in model_config}
    forbidden = sorted(lowered & FORBIDDEN_MODEL_CONFIG_KEYS)
    if forbidden:
        raise ConfigError(
            "credential material must not be configured on dbt models: " + ", ".join(forbidden)
        )
